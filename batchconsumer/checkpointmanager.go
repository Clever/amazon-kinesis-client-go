package batchconsumer

import (
	"time"

	kv "gopkg.in/Clever/kayvee-go.v6/logger"

	"github.com/Clever/amazon-kinesis-client-go/batchconsumer/stats"
	"github.com/Clever/amazon-kinesis-client-go/kcl"
)

type checkpointManager struct {
	log kv.KayveeLogger

	checkpointFreq time.Duration

	checkpoint chan kcl.SequencePair
	shutdown   chan chan<- struct{}
}

func NewCheckpointManager(
	checkpointer kcl.Checkpointer, config Config, log kv.KayveeLogger,
) *checkpointManager {
	cm := &checkpointManager{
		log: log,

		checkpointFreq: config.CheckpointFreq,

		checkpoint: make(chan kcl.SequencePair),
		shutdown:   make(chan chan<- struct{}),
	}

	cm.startCheckpointHandler(checkpointer, cm.checkpoint, cm.shutdown)

	return cm
}

func (cm *checkpointManager) Checkpoint(pair kcl.SequencePair) {
	cm.checkpoint <- pair
}

func (cm *checkpointManager) Shutdown() <-chan struct{} {
	done := make(chan struct{})
	cm.shutdown <- done

	return done
}

func (cm *checkpointManager) startCheckpointHandler(
	checkpointer kcl.Checkpointer, checkpoint <-chan kcl.SequencePair,
	shutdown <-chan chan<- struct{},
) {
	go func() {
		lastCheckpoint := time.Now()

		for {
			var doneShutdown chan<- struct{}
			pair := kcl.SequencePair{}

			select {
			case pair = <-checkpoint:
			case doneShutdown = <-shutdown:
			}

			// This is a write throttle to ensure we don't checkpoint faster than cm.checkpointFreq.
			// The latest pair number is always used.
			for doneShutdown == nil && time.Now().Sub(lastCheckpoint) < cm.checkpointFreq {
				select {
				case pair = <-checkpoint: // Keep updating checkpoint pair while waiting
				case doneShutdown = <-shutdown:
				case <-time.NewTimer(cm.checkpointFreq - time.Now().Sub(lastCheckpoint)).C:
				}
			}

			if !pair.IsEmpty() {
				checkpointer.Checkpoint(pair)
				lastCheckpoint = time.Now()
				stats.Counter("checkpoints-sent", 1)
			}

			if doneShutdown != nil {
				checkpointer.Shutdown()
				doneShutdown <- struct{}{}
				return
			}
		}
	}()
}
