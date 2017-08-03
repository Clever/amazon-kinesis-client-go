package batchconsumer

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/big"
	"os"
	"time"

	"golang.org/x/time/rate"
	kv "gopkg.in/Clever/kayvee-go.v6/logger"

	"github.com/Clever/amazon-kinesis-client-go/batchconsumer/batcher"
	"github.com/Clever/amazon-kinesis-client-go/kcl"
	"github.com/Clever/amazon-kinesis-client-go/splitter"
)

type tagMsgPair struct {
	tag  string
	msg  []byte
	pair kcl.SequencePair
}

type batchedWriter struct {
	config Config
	sender Sender
	log    kv.KayveeLogger

	shardID string

	checkpointMsg      chan kcl.SequencePair
	checkpointShutdown chan struct{}
	checkpointTag      chan string
	lastIgnoredPair    chan kcl.SequencePair
	batchMsg           chan tagMsgPair
	shutdown           chan struct{}

	// Limits the number of records read from the stream
	rateLimiter *rate.Limiter

	lastProcessedSeq kcl.SequencePair
}

func NewBatchedWriter(config Config, sender Sender, log kv.KayveeLogger) *batchedWriter {
	return &batchedWriter{
		config: config,
		sender: sender,
		log:    log,

		rateLimiter: rate.NewLimiter(rate.Limit(config.ReadRateLimit), config.ReadBurstLimit),
	}
}

func (b *batchedWriter) Initialize(shardID string, checkpointer kcl.Checkpointer) error {
	b.shardID = shardID
	b.checkpointMsg = make(chan kcl.SequencePair)
	b.checkpointShutdown = make(chan struct{})
	b.startCheckpointListener(checkpointer, b.checkpointMsg, b.checkpointShutdown)

	b.checkpointTag = make(chan string, 100) // Buffered to workaround
	b.batchMsg = make(chan tagMsgPair)
	b.shutdown = make(chan struct{})
	b.lastIgnoredPair = make(chan kcl.SequencePair)
	b.startMessageHandler(b.batchMsg, b.checkpointTag, b.lastIgnoredPair, b.shutdown)

	return nil
}

func (b *batchedWriter) startCheckpointListener(
	checkpointer kcl.Checkpointer, checkpointMsg <-chan kcl.SequencePair,
	shutdown <-chan struct{},
) {
	go func() {
		lastCheckpoint := time.Now()

		for {
			pair := kcl.SequencePair{}
			isShuttingDown := false

			select {
			case pair = <-checkpointMsg:
			case <-shutdown:
				isShuttingDown = true
			}

			// This is a write throttle to ensure we don't checkpoint faster than
			// b.config.CheckpointFreq.  The latest pair number is always used.
			for !isShuttingDown && time.Now().Sub(lastCheckpoint) < b.config.CheckpointFreq {
				select {
				case pair = <-checkpointMsg: // Keep updating checkpoint pair while waiting
				case <-shutdown:
					isShuttingDown = true
				case <-time.NewTimer(b.config.CheckpointFreq - time.Now().Sub(lastCheckpoint)).C:
				}
			}

			if !pair.IsEmpty() {
				err := checkpointer.Checkpoint(pair, b.config.CheckpointRetries)
				if err != nil {
					b.log.ErrorD("checkpoint-err", kv.M{"msg": err.Error(), "shard-id": b.shardID})
				} else {
					lastCheckpoint = time.Now()
				}
			}

			if isShuttingDown {
				checkpointer.Shutdown()
				return
			}
		}
	}()
}

func (b *batchedWriter) createBatcher(tag string) batcher.Batcher {
	sync := &batcherSync{
		tag:    tag,
		writer: b,
	}
	batch, err := batcher.New(sync, b.config.BatchInterval, b.config.BatchCount, b.config.BatchSize)
	if err != nil {
		b.log.ErrorD("create-batcher", kv.M{"msg": err.Error(), "tag": tag})
	}

	return batch
}

// startMessageDistributer starts a go-routine that routes messages to batches.  It's in uses a
// go routine to avoid racey conditions.
func (b *batchedWriter) startMessageHandler(
	batchMsg <-chan tagMsgPair, checkpointTag <-chan string, lastIgnored <-chan kcl.SequencePair,
	shutdown <-chan struct{},
) {
	getBatcher := make(chan string)
	rtnBatcher := make(chan batcher.Batcher)
	shutdownAdder := make(chan struct{})

	go func() {
		for {
			select {
			case tmp := <-batchMsg:
				getBatcher <- tmp.tag
				batcher := <-rtnBatcher
				err := batcher.AddMessage(tmp.msg, tmp.pair)
				if err != nil {
					b.log.ErrorD("add-message", kv.M{
						"err": err.Error(), "msg": string(tmp.msg), "tag": tmp.tag,
					})
				}
			case <-shutdownAdder:
			}
		}
	}()

	go func() {
		var lastIgnoredPair kcl.SequencePair
		batchers := map[string]batcher.Batcher{}
		areBatchersEmpty := true

		for {
			select {
			case tag := <-getBatcher:
				batcher, ok := batchers[tag]
				if !ok {
					batcher = b.createBatcher(tag)
					batchers[tag] = batcher
				}

				areBatchersEmpty = false
				rtnBatcher <- batcher
			case tag := <-checkpointTag:
				smallest := lastIgnoredPair
				isAllEmpty := true

				for name, batch := range batchers {
					if tag == name {
						continue
					}

					pair := batch.SmallestSequencePair()
					if pair.IsEmpty() { // Occurs when batch has no items
						continue
					}

					// Check for empty because it's possible that no messages have been ignored
					if smallest.IsEmpty() || pair.IsLessThan(smallest) {
						smallest = pair
					}

					isAllEmpty = false
				}

				if !smallest.IsEmpty() {
					b.checkpointMsg <- smallest
				}
				areBatchersEmpty = isAllEmpty
			case pair := <-lastIgnored:
				if areBatchersEmpty && !pair.IsEmpty() {
					b.checkpointMsg <- pair
				}
				lastIgnoredPair = pair
			case <-shutdown:
				for _, batch := range batchers {
					batch.Flush()
				}
				b.checkpointMsg <- b.lastProcessedSeq
				b.checkpointShutdown <- struct{}{}

				areBatchersEmpty = true
			}
		}
	}()
}

func (b *batchedWriter) splitMessageIfNecessary(record []byte) ([][]byte, error) {
	// We handle two types of records:
	// - records emitted from CWLogs Subscription
	// - records emiited from KPL
	if !splitter.IsGzipped(record) {
		// Process a single message, from KPL
		return [][]byte{record}, nil
	}

	// Process a batch of messages from a CWLogs Subscription
	return splitter.GetMessagesFromGzippedInput(record, b.config.DeployEnv == "production")
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

		pair = kcl.SequencePair{seq, record.SubSequenceNumber}
		if prevPair.IsEmpty() { // Handles on-start edge case where b.lastProcessSeq is empty
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
				b.log.ErrorD("process-message", kv.M{"msg": err.Error(), "rawmsg": string(rawmsg)})
				continue // Don't stop processing messages because of one bad message
			}

			if len(tags) == 0 {
				b.log.ErrorD("no-tags", kv.M{"rawmsg": string(rawmsg)})
				return fmt.Errorf("No tags provided by consumer for log: %s", string(rawmsg))
			}

			for _, tag := range tags {
				if tag == "" {
					b.log.ErrorD("blank-tag", kv.M{"rawmsg": string(rawmsg)})
					return fmt.Errorf("Blank tag provided by consumer for log: %s", string(rawmsg))
				}

				// Use second to last sequence number to ensure we don't checkpoint a message before
				// it's been sent.  When batches are sent, conceptually we first find the smallest
				// sequence number amount all the batch (let's call it A).  We then checkpoint at
				// the A-1 sequence number.
				b.batchMsg <- tagMsgPair{tag, msg, prevPair}
				wasPairIgnored = false
			}
		}

		prevPair = pair
		if wasPairIgnored {
			b.lastIgnoredPair <- pair
		}
	}
	b.lastProcessedSeq = pair

	return nil
}

func (b *batchedWriter) SendBatch(batch [][]byte, tag string) {
	err := b.sender.SendBatch(batch, tag)
	switch e := err.(type) {
	case nil: // Do nothing
	case PartialSendBatchError:
		b.log.ErrorD("send-batch", kv.M{"msg": e.Error()})
		for _, line := range e.FailedMessages {
			b.log.ErrorD("failed-log", kv.M{"log": line})
		}
	case CatastrophicSendBatchError:
		b.log.CriticalD("send-batch", kv.M{"msg": e.Error()})
		os.Exit(1)
	default:
		b.log.CriticalD("send-batch", kv.M{"msg": e.Error()})
		os.Exit(1)
	}

	b.checkpointTag <- tag
}

func (b *batchedWriter) Shutdown(reason string) error {
	if reason == "TERMINATE" {
		b.log.InfoD("terminate-signal", kv.M{"shard-id": b.shardID})
	} else {
		b.log.ErrorD("shutdown-failover", kv.M{"shard-id": b.shardID, "reason": reason})
	}
	b.shutdown <- struct{}{}
	return nil
}
