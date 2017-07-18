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

type BatchedWriter struct {
	config Config
	sender Sender
	log    kv.KayveeLogger

	shardID        string
	checkpointChan chan batcher.SequencePair

	// Limits the number of records read from the stream
	rateLimiter *rate.Limiter

	batchers         map[string]batcher.Batcher
	lastProcessedSeq batcher.SequencePair
}

func (b *BatchedWriter) Initialize(shardID string, checkpointer *kcl.Checkpointer) error {
	b.batchers = map[string]batcher.Batcher{}
	b.shardID = shardID
	b.checkpointChan = make(chan batcher.SequencePair)
	b.rateLimiter = rate.NewLimiter(rate.Limit(b.config.ReadRateLimit), b.config.ReadBurstLimit)

	b.startCheckpointListener(checkpointer, b.checkpointChan)

	return nil
}

// handleCheckpointError returns true if checkout should be tried again.  Returns false otherwise.
func (b *BatchedWriter) handleCheckpointError(err error) bool {
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

func (b *BatchedWriter) startCheckpointListener(
	checkpointer *kcl.Checkpointer, checkpointChan <-chan batcher.SequencePair,
) {
	lastCheckpoint := time.Now()

	go func() {
		for {
			seq := <-checkpointChan

			// This is a write throttle to ensure we don't checkpoint faster than
			// b.config.CheckpointFreq.  The latest seq number is always used.
			for time.Now().Sub(lastCheckpoint) < b.config.CheckpointFreq {
				select {
				case seq = <-checkpointChan: // Keep updating checkpoint seq while waiting
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

func (b *BatchedWriter) createBatcher(tag string) batcher.Batcher {
	sync := &BatcherSync{
		tag:    tag,
		writer: b,
	}
	return batcher.New(sync, b.config.FlushInterval, b.config.FlushCount, b.config.FlushSize)
}

func (b *BatchedWriter) splitMessageIfNecessary(record []byte) ([][]byte, error) {
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

func (b *BatchedWriter) ProcessRecords(records []kcl.Record) error {
	curSequence := b.lastProcessedSeq

	for _, record := range records {
		// Wait until rate limiter permits one more record to be processed
		b.rateLimiter.Wait(context.Background())

		seq := new(big.Int)
		if _, ok := seq.SetString(record.SequenceNumber, 10); !ok { // Validating sequence
			return fmt.Errorf("could not parse sequence number '%s'", record.SequenceNumber)
		}

		b.lastProcessedSeq = curSequence // Updated with the value from the previous iteration
		curSequence = batcher.SequencePair{seq, record.SubSequenceNumber}

		data, err := base64.StdEncoding.DecodeString(record.Data)
		if err != nil {
			return err
		}

		rawlogs, err := b.splitMessageIfNecessary(data)
		if err != nil {
			return err
		}
		for _, rawlog := range rawlogs {
			log, tags, err := b.sender.EncodeLog(rawlog)
			if err == ErrLogIgnored {
				continue // Skip message
			} else if err != nil {
				return err
			}

			if len(tags) == 0 {
				return fmt.Errorf("No tags provided by consumer for log: %s", string(rawlog))
			}

			for _, tag := range tags {
				batcher, ok := b.batchers[tag]
				if !ok {
					batcher = b.createBatcher(tag)
					b.batchers[tag] = batcher
				}

				// Use second to last sequence number to ensure we don't checkpoint a message before
				// it's been sent.  When batches are sent, conceptually we first find the smallest
				// sequence number amount all the batch (let's call it A).  We then checkpoint at
				// the A-1 sequence number.
				err = batcher.AddMessage(log, b.lastProcessedSeq)
				if err != nil {
					return err
				}
			}
		}
	}

	b.lastProcessedSeq = curSequence

	return nil
}

func (b *BatchedWriter) CheckPointBatch(tag string) {
	smallest := b.lastProcessedSeq

	for name, batch := range b.batchers {
		if tag == name {
			continue
		}

		pair := batch.SmallestSequencePair()
		if pair.Sequence == nil { // Occurs when batch has no items
			continue
		}

		isSmaller := smallest.Sequence == nil || // smallest.Sequence means batch just flushed
			pair.Sequence.Cmp(smallest.Sequence) == -1 ||
			(pair.Sequence.Cmp(smallest.Sequence) == 0 && pair.SubSequence < smallest.SubSequence)
		if isSmaller {
			smallest = pair
		}
	}

	b.checkpointChan <- smallest
}

func (b *BatchedWriter) SendBatch(batch [][]byte, tag string) {
	b.log.Info("sent-batch")
	err := b.sender.SendBatch(batch, tag)
	switch e := err.(type) {
	case nil: // Do nothing
	case PartialOutputError:
		b.log.ErrorD("send-batch", kv.M{"msg": e.Error()})
		for _, line := range e.Logs {
			b.log.ErrorD("failed-log", kv.M{"log": line})
		}
	case CatastrophicOutputError:
		b.log.CriticalD("send-batch", kv.M{"msg": e.Error()})
		os.Exit(1)
	default:
		b.log.CriticalD("send-batch", kv.M{"msg": e.Error()})
		os.Exit(1)
	}
}

func (b *BatchedWriter) Shutdown(reason string) error {
	if reason == "TERMINATE" {
		b.log.InfoD("terminate-signal", kv.M{"shard-id": b.shardID})
		for _, batch := range b.batchers {
			batch.Flush()
		}
	} else {
		b.log.ErrorD("shutdown-failover", kv.M{"shard-id": b.shardID, "reason": reason})
	}
	return nil
}
