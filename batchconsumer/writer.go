package batchconsumer

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math/big"

	"golang.org/x/time/rate"
	kv "gopkg.in/Clever/kayvee-go.v6/logger"

	"github.com/Clever/amazon-kinesis-client-go/batchconsumer/stats"
	"github.com/Clever/amazon-kinesis-client-go/kcl"
	"github.com/Clever/amazon-kinesis-client-go/splitter"
)

type batchedWriter struct {
	config         Config
	sender         Sender
	failedLogsFile kv.KayveeLogger

	shardID string

	chkpntManager  *checkpointManager
	batcherManager *batcherManager

	// Limits the number of records read from the stream
	rateLimiter *rate.Limiter

	lastProcessedSeq kcl.SequencePair
}

func NewBatchedWriter(config Config, sender Sender, failedLogsFile kv.KayveeLogger) *batchedWriter {
	return &batchedWriter{
		config:         config,
		sender:         sender,
		failedLogsFile: failedLogsFile,

		rateLimiter: rate.NewLimiter(rate.Limit(config.ReadRateLimit), config.ReadBurstLimit),
	}
}

func (b *batchedWriter) Initialize(shardID string, checkpointer kcl.Checkpointer) error {
	b.shardID = shardID

	bmConfig := batcherManagerConfig{
		BatchCount:    b.config.BatchCount,
		BatchSize:     b.config.BatchSize,
		BatchInterval: b.config.BatchInterval,
	}

	b.sender.Initialize(shardID)

	b.chkpntManager = newCheckpointManager(checkpointer, b.config.CheckpointFreq)
	b.batcherManager = newBatcherManager(b.sender, b.chkpntManager, bmConfig, b.failedLogsFile)

	return nil
}

func (b *batchedWriter) splitMessageIfNecessary(record []byte) ([][]byte, error) {
	// We handle three types of records:
	// - records emitted from CWLogs Subscription (which are gzip compressed)
	// - uncompressed records emitted from KPL
	// - zlib compressed records (e.g. as compressed and emitted by Kinesis plugin for Fluent Bit)
	if splitter.IsGzipped(record) {
		// Process a batch of messages from a CWLogs Subscription
		return splitter.GetMessagesFromGzippedInput(record)
	}

	// Try to read it as a zlib-compressed record
	// zlib.NewReader checks for a zlib header and returns an error if not found
	zlibReader, err := zlib.NewReader(bytes.NewReader(record))
	if err == nil {
		unzlibRecord, err := ioutil.ReadAll(zlibReader)
		if err != nil {
			return nil, fmt.Errorf("reading zlib-compressed record: %v", err)
		}
		return [][]byte{unzlibRecord}, nil
	}
	// Process a single message, from KPL
	return [][]byte{record}, nil

}

func (b *batchedWriter) ProcessRecords(records []kcl.Record) error {
	var pair kcl.SequencePair
	prevPair := b.lastProcessedSeq

	for _, record := range records {
		// Wait until rate limiter permits one more record to be processed
		b.rateLimiter.Wait(context.Background())

		seq := new(big.Int)
		if _, ok := seq.SetString(record.SequenceNumber, 10); !ok { // Validating sequence
			return fmt.Errorf("could not parse sequence number '%s'", record.SequenceNumber)
		}

		pair = kcl.SequencePair{Sequence: seq, SubSequence: record.SubSequenceNumber}
		if prevPair.IsNil() { // Handles on-start edge case where b.lastProcessSeq is empty
			prevPair = pair
		}

		data, err := base64.StdEncoding.DecodeString(record.Data)
		if err != nil {
			return err
		}

		messages, err := b.splitMessageIfNecessary(data)
		if err != nil {
			return err
		}
		wasPairIgnored := true
		for _, rawmsg := range messages {
			msg, tags, err := b.sender.ProcessMessage(rawmsg)

			if err == ErrMessageIgnored {
				continue // Skip message
			} else if err != nil {
				stats.Counter("unknown-error", 1)
				lg.ErrorD("process-message", kv.M{"msg": err.Error(), "rawmsg": string(rawmsg)})
				continue // Don't stop processing messages because of one bad message
			}

			if len(tags) == 0 {
				stats.Counter("no-tags", 1)
				lg.ErrorD("no-tags", kv.M{"rawmsg": string(rawmsg)})
				return fmt.Errorf("No tags provided by consumer for log: %s", string(rawmsg))
			}

			for _, tag := range tags {
				if tag == "" {
					stats.Counter("blank-tag", 1)
					lg.ErrorD("blank-tag", kv.M{"rawmsg": string(rawmsg)})
					return fmt.Errorf("Blank tag provided by consumer for log: %s", string(rawmsg))
				}

				// Use second to last sequence number to ensure we don't checkpoint a message before
				// it's been sent.  When batches are sent, conceptually we first find the smallest
				// sequence number amount all the batch (let's call it A).  We then checkpoint at
				// the A-1 sequence number.
				b.batcherManager.BatchMessage(tag, msg, prevPair)
				wasPairIgnored = false
			}
		}

		prevPair = pair
		if wasPairIgnored {
			b.batcherManager.LatestIgnored(pair)
		}
		b.batcherManager.LatestProcessed(pair)

		stats.Counter("processed-messages", len(messages))
	}
	b.lastProcessedSeq = pair

	return nil
}

func (b *batchedWriter) Shutdown(reason string) error {
	if reason == "TERMINATE" {
		lg.InfoD("terminate-signal", kv.M{"shard-id": b.shardID})
	} else {
		lg.ErrorD("shutdown-failover", kv.M{"shard-id": b.shardID, "reason": reason})
	}

	done := b.batcherManager.Shutdown()
	<-done

	return nil
}
