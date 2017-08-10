package batchconsumer

import (
	"os"
	"time"

	kv "gopkg.in/Clever/kayvee-go.v6/logger"

	"github.com/Clever/amazon-kinesis-client-go/batchconsumer/stats"
	"github.com/Clever/amazon-kinesis-client-go/kcl"
)

type tagMsgPair struct {
	tag  string
	msg  []byte
	pair kcl.SequencePair
}

type batcherManager struct {
	log           kv.KayveeLogger
	sender        Sender
	chkpntManager *checkpointManager

	batchCount    int
	batchSize     int
	batchInterval time.Duration

	batchMsg      chan tagMsgPair
	lastIgnored   chan kcl.SequencePair
	lastProcessed chan kcl.SequencePair
	shutdown      chan chan<- struct{}
}

func newBatcherManager(
	sender Sender, chkpntManager *checkpointManager, config Config, log kv.KayveeLogger,
) *batcherManager {
	bm := &batcherManager{
		log:           log,
		sender:        sender,
		chkpntManager: chkpntManager,

		batchCount:    config.BatchCount,
		batchSize:     config.BatchSize,
		batchInterval: config.BatchInterval,

		batchMsg:      make(chan tagMsgPair),
		lastIgnored:   make(chan kcl.SequencePair),
		lastProcessed: make(chan kcl.SequencePair),
		shutdown:      make(chan chan<- struct{}),
	}

	bm.startMessageHandler(bm.batchMsg, bm.lastIgnored, bm.lastProcessed, bm.shutdown)

	return bm
}

func (b *batcherManager) BatchMessage(tag string, msg []byte, pair kcl.SequencePair) {
	b.batchMsg <- tagMsgPair{tag, msg, pair}
}

func (b *batcherManager) LatestIgnored(pair kcl.SequencePair) {
	b.lastIgnored <- pair
}

func (b *batcherManager) LatestProcessed(pair kcl.SequencePair) {
	b.lastProcessed <- pair
}

func (b *batcherManager) Shutdown() <-chan struct{} {
	done := make(chan struct{})
	b.shutdown <- done

	return done
}

func (b *batcherManager) createBatcher() *batcher {
	return &batcher{
		flushCount: b.batchCount,
		flushSize:  b.batchSize,
	}
}

func (b *batcherManager) sendBatch(batcher *batcher, tag string) {
	if len(batcher.Batch) <= 0 {
		return
	}

	err := b.sender.SendBatch(batcher.Batch, tag)
	switch e := err.(type) {
	case nil: // Do nothing
	case PartialSendBatchError:
		b.log.ErrorD("send-batch", kv.M{"msg": e.Error()})
		for _, line := range e.FailedMessages {
			b.log.ErrorD("failed-log", kv.M{"log": line})
		}
		stats.Counter("batch-log-failures", len(e.FailedMessages))
	case CatastrophicSendBatchError:
		b.log.CriticalD("send-batch", kv.M{"msg": e.Error()})
		os.Exit(1)
	default:
		b.log.CriticalD("send-batch", kv.M{"msg": e.Error()})
		os.Exit(1)
	}

	batcher.Clear()
	stats.Counter("batches-sent", 1)
}

func (b *batcherManager) sendCheckpoint(
	tag string, lastIgnoredPair kcl.SequencePair, batchers map[string]*batcher,
) {
	smallest := lastIgnoredPair

	for name, batcher := range batchers {
		if tag == name {
			continue
		}

		if len(batcher.Batch) <= 0 {
			continue
		}

		// Check for empty because it's possible that no messages have been ignored
		if smallest.IsEmpty() || batcher.SmallestSeq.IsLessThan(smallest) {
			smallest = batcher.SmallestSeq
		}
	}

	if !smallest.IsEmpty() {
		b.chkpntManager.Checkpoint(smallest)
	}
}

// startMessageDistributer starts a go-routine that routes messages to batches.  It's in uses a
// go routine to avoid racey conditions.
func (b *batcherManager) startMessageHandler(
	batchMsg <-chan tagMsgPair, lastIgnored, lastProcessed <-chan kcl.SequencePair,
	shutdown <-chan chan<- struct{},
) {
	go func() {
		var lastProcessedPair kcl.SequencePair
		var lastIgnoredPair kcl.SequencePair
		batchers := map[string]*batcher{}

		for {
			for tag, batcher := range batchers { // Flush batcher that hasn't been updated recently
				if b.batchInterval <= time.Now().Sub(batcher.LastUpdated) {
					b.sendBatch(batcher, tag)
					b.sendCheckpoint(tag, lastIgnoredPair, batchers)
					batcher.Clear()
				}
			}

			select {
			case <-time.NewTimer(time.Second).C:
				// Timer is janky way to ensure the code above is ran at regular intervals
				// Can't put above code under this case because it's very possible that this
				// timer is always pre-empted by other channels
			case tmp := <-batchMsg:
				batcher, ok := batchers[tmp.tag]
				if !ok {
					batcher = b.createBatcher()
					batchers[tmp.tag] = batcher
					stats.Gauge("tag-count", len(batchers))
				}

				err := batcher.AddMessage(tmp.msg, tmp.pair)
				if err == ErrBatchFull {
					b.sendBatch(batcher, tmp.tag)
					b.sendCheckpoint(tmp.tag, lastIgnoredPair, batchers)

					batcher.AddMessage(tmp.msg, tmp.pair)
				} else if err != nil {
					b.log.ErrorD("add-message", kv.M{
						"err": err.Error(), "msg": string(tmp.msg), "tag": tmp.tag,
					})
				}
				stats.Counter("msg-batched", 1)
			case pair := <-lastIgnored:
				lastIgnoredPair = pair

				isPendingMessages := false
				for _, batcher := range batchers {
					if len(batcher.Batch) > 0 {
						isPendingMessages = true
						break
					}
				}

				if !isPendingMessages {
					b.chkpntManager.Checkpoint(lastIgnoredPair)
				}
			case pair := <-lastProcessed:
				lastProcessedPair = pair
			case done := <-shutdown:
				for tag, batcher := range batchers {
					b.sendBatch(batcher, tag)
				}
				b.chkpntManager.Checkpoint(lastProcessedPair)
				chkDone := b.chkpntManager.Shutdown()
				<-chkDone

				done <- struct{}{}
				return
			}
		}
	}()
}
