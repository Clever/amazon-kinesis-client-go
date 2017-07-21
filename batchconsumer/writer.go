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
	pair batcher.SequencePair
}

type batchedWriter struct {
	config Config
	sender Sender
	log    kv.KayveeLogger

	shardID string

	checkpointMsg     chan batcher.SequencePair
	checkpointTag     chan string
	lastProcessedPair chan batcher.SequencePair
	batchMsg          chan tagMsgPair
	flushBatches      chan struct{}

	// Limits the number of records read from the stream
	rateLimiter *rate.Limiter

	lastProcessedSeq batcher.SequencePair
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
	b.checkpointMsg = make(chan batcher.SequencePair)
	b.startCheckpointListener(checkpointer, b.checkpointMsg)

	b.checkpointTag = make(chan string)
	b.batchMsg = make(chan tagMsgPair)
	b.flushBatches = make(chan struct{})
	b.lastProcessedPair = make(chan batcher.SequencePair)
	b.startMessageHandler(b.batchMsg, b.checkpointTag, b.lastProcessedPair, b.flushBatches)

	return nil
}

// handleCheckpointError returns true if checkout should be tried again.  Returns false otherwise.
func (b *batchedWriter) handleCheckpointError(err error) bool {
	if err == nil {
		return false
	}

	cperr, ok := err.(kcl.CheckpointError)
	if !ok {
		b.log.ErrorD("unknown-checkpoint-error", kv.M{"msg": err.Error(), "shard-id": b.shardID})
		return true
	}

	switch cperr.Error() {
	case "ShutdownException": // Skips checkpointing
		b.log.ErrorD("shutdown-checkpoint-exception", kv.M{
			"msg": err.Error(), "shard-id": b.shardID,
		})
		return false
	case "ThrottlingException":
		b.log.ErrorD("checkpoint-throttle", kv.M{"shard-id": b.shardID})
	case "InvalidStateException":
		b.log.ErrorD("invalid-checkpoint-state", kv.M{"shard-id": b.shardID})
	default:
		b.log.ErrorD("checkpoint-error", kv.M{"shard-id": b.shardID, "msg": err})
	}

	return true
}

func (b *batchedWriter) startCheckpointListener(
	checkpointer kcl.Checkpointer, checkpointMsg <-chan batcher.SequencePair,
) {
	go func() {
		lastCheckpoint := time.Now()

		for {
			seq := <-checkpointMsg

			// This is a write throttle to ensure we don't checkpoint faster than
			// b.config.CheckpointFreq.  The latest seq number is always used.
			for time.Now().Sub(lastCheckpoint) < b.config.CheckpointFreq {
				select {
				case seq = <-checkpointMsg: // Keep updating checkpoint seq while waiting
				case <-time.NewTimer(b.config.CheckpointFreq - time.Now().Sub(lastCheckpoint)).C:
				}
			}

			retry := true
			for n := 0; retry && n < b.config.CheckpointRetries+1; n++ {
				str := seq.Sequence.String()
				err := checkpointer.Checkpoint(&str, &seq.SubSequence)
				if err == nil { // Successfully checkpointed!
					lastCheckpoint = time.Now()
					break
				}

				retry = b.handleCheckpointError(err)

				if n == b.config.CheckpointRetries {
					b.log.ErrorD("checkpoint-retries", kv.M{"attempts": b.config.CheckpointRetries})
					retry = false
				}

				if retry {
					time.Sleep(b.config.CheckpointRetrySleep)
				}
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
	batchMsg <-chan tagMsgPair, checkpointTag <-chan string, lastPair <-chan batcher.SequencePair,
	flushBatches <-chan struct{},
) {
	go func() {
		var lastProcessedPair batcher.SequencePair
		batchers := map[string]batcher.Batcher{}
		areBatchersEmpty := true

		for {
			select {
			case tmp := <-batchMsg:
				batcher, ok := batchers[tmp.tag]
				if !ok {
					batcher = b.createBatcher(tmp.tag)
					batchers[tmp.tag] = batcher
				}

				err := batcher.AddMessage(tmp.msg, tmp.pair)
				if err != nil {
					b.log.ErrorD("add-message", kv.M{
						"err": err.Error(), "msg": string(tmp.msg), "tag": tmp.tag,
					})
				}
				areBatchersEmpty = false
			case tag := <-checkpointTag:
				smallest := lastProcessedPair
				isAllEmpty := true

				for name, batch := range batchers {
					if tag == name {
						continue
					}

					pair := batch.SmallestSequencePair()
					if pair.IsEmpty() { // Occurs when batch has no items
						continue
					}

					if pair.IsLessThan(smallest) {
						smallest = pair
					}

					isAllEmpty = false
				}

				if !smallest.IsEmpty() {
					b.checkpointMsg <- smallest
				}
				areBatchersEmpty = isAllEmpty
			case pair := <-lastPair:
				if areBatchersEmpty {
					b.checkpointMsg <- pair
				}
				lastProcessedPair = pair
			case <-flushBatches:
				for _, batch := range batchers {
					batch.Flush()
				}
				b.checkpointMsg <- lastProcessedPair
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
	var pair batcher.SequencePair
	prevPair := b.lastProcessedSeq

	for _, record := range records {
		// Wait until rate limiter permits one more record to be processed
		b.rateLimiter.Wait(context.Background())

		seq := new(big.Int)
		if _, ok := seq.SetString(record.SequenceNumber, 10); !ok { // Validating sequence
			return fmt.Errorf("could not parse sequence number '%s'", record.SequenceNumber)
		}

		pair = batcher.SequencePair{seq, record.SubSequenceNumber}
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
			}
		}

		prevPair = pair
		b.lastProcessedPair <- pair
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
		b.flushBatches <- struct{}{}
	} else {
		b.log.ErrorD("shutdown-failover", kv.M{"shard-id": b.shardID, "reason": reason})
	}
	return nil
}
