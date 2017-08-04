package batchconsumer

import (
	"time"

	kv "gopkg.in/Clever/kayvee-go.v6/logger"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
)

type checkpointManager struct {
	log kv.KayveeLogger

	checkpointRetries int
	checkpointFreq    time.Duration

	checkpoint chan kcl.SequencePair
	shutdown   chan struct{}
}

func NewCheckpointManager(
	checkpointer kcl.Checkpointer, config Config, log kv.KayveeLogger,
) *checkpointManager {
	cm := &checkpointManager{
		log: log,

		checkpointRetries: config.CheckpointRetries,
		checkpointFreq:    config.CheckpointFreq,

		checkpoint: make(chan kcl.SequencePair),
		shutdown:   make(chan struct{}),
	}

	cm.startCheckpointHandler(checkpointer, cm.checkpoint, cm.shutdown)

	return cm
}

func (cm *checkpointManager) Checkpoint(pair kcl.SequencePair) {
	cm.checkpoint <- pair
}

func (cm *checkpointManager) Shutdown() {
	cm.shutdown <- struct{}{}
}

func (cm *checkpointManager) startCheckpointHandler(
	checkpointer kcl.Checkpointer, checkpoint <-chan kcl.SequencePair, shutdown <-chan struct{},
) {
	go func() {
		lastCheckpoint := time.Now()

		for {
			pair := kcl.SequencePair{}
			isShuttingDown := false

			select {
			case pair = <-checkpoint:
			case <-shutdown:
				isShuttingDown = true
			}

			// This is a write throttle to ensure we don't checkpoint faster than cm.checkpointFreq.
			// The latest pair number is always used.
			for !isShuttingDown && time.Now().Sub(lastCheckpoint) < cm.checkpointFreq {
				select {
				case pair = <-checkpoint: // Keep updating checkpoint pair while waiting
				case <-shutdown:
					isShuttingDown = true
				case <-time.NewTimer(cm.checkpointFreq - time.Now().Sub(lastCheckpoint)).C:
				}
			}

			if !pair.IsEmpty() {
				err := checkpointer.Checkpoint(pair, cm.checkpointRetries)
				if err != nil {
					cm.log.ErrorD("checkpoint-err", kv.M{"msg": err.Error()})
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
