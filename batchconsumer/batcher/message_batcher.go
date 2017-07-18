package batcher

import (
	"fmt"
	"math/big"
	"sync"
	"time"
)

// SequencePair a convience way to pass around a Sequence / SubSequence pair
type SequencePair struct {
	Sequence    *big.Int
	SubSequence int
}

func (s SequencePair) IsLessThan(pair SequencePair) bool {
	if s.Sequence == nil {
		return false
	}
	if pair.Sequence == nil {
		return true
	}

	cmp := s.Sequence.Cmp(pair.Sequence)
	if cmp == -1 {
		return true
	}
	if cmp == 1 {
		return false
	}

	return s.SubSequence < pair.SubSequence
}

// Sync is used to allow a writer to syncronize with the batcher.
// The writer declares how to write messages (via its `SendBatch` method), while the batcher
// keeps track of messages written
type Sync interface {
	SendBatch(batch [][]byte)
}

// Batcher interface
type Batcher interface {
	// AddMesage to the batch
	AddMessage(msg []byte, sequencePair SequencePair) error
	// Flush all messages from the batch
	Flush()
	// SmallestSeqPair returns the smallest SequenceNumber and SubSequence number in
	// the current batch
	SmallestSequencePair() SequencePair
}

type msgPack struct {
	msg          []byte
	sequencePair SequencePair
}

type batcher struct {
	mux sync.Mutex

	flushInterval time.Duration
	flushCount    int
	flushSize     int

	// smallestSeq and smallestSubSeq are used to track the highest sequence number
	// of any record in the batch. This is used for checkpointing.
	smallestSeq SequencePair

	sync      Sync
	msgChan   chan<- msgPack
	flushChan chan<- struct{}
}

// New creates a new Batcher
// - sync - synchronizes batcher with writer
// - flushInterval - how often accumulated messages should be flushed (default 1 second).
// - flushCount - number of messages that trigger a flush (default 10).
// - flushSize - size of batch that triggers a flush (default 1024 * 1024 = 1 mb)
func New(sync Sync, flushInterval time.Duration, flushCount int, flushSize int) Batcher {
	msgChan := make(chan msgPack, 100)
	flushChan := make(chan struct{})

	b := &batcher{
		flushCount:    flushCount,
		flushInterval: flushInterval,
		flushSize:     flushSize,
		sync:          sync,
		msgChan:       msgChan,
		flushChan:     flushChan,
	}

	go b.startBatcher(msgChan, flushChan)

	return b
}

func (b *batcher) SmallestSequencePair() SequencePair {
	b.mux.Lock()
	defer b.mux.Unlock()

	return b.smallestSeq
}

func (b *batcher) SetFlushInterval(dur time.Duration) {
	b.flushInterval = dur
}

func (b *batcher) SetFlushCount(count int) {
	b.flushCount = count
}

func (b *batcher) SetFlushSize(size int) {
	b.flushSize = size
}

func (b *batcher) AddMessage(msg []byte, pair SequencePair) error {
	if len(msg) <= 0 {
		return fmt.Errorf("Empty messages can't be sent")
	}

	b.msgChan <- msgPack{msg, pair}
	return nil
}

// updateSequenceNumbers is used to track the smallest sequenceNumber of any record in the batch.
// When flush() is called, the batcher sends the sequence number to the writer. When the writer
// checkpoints, it does so up to the latest message that was flushed successfully.
func (b *batcher) updateSequenceNumbers(pair SequencePair) {
	b.mux.Lock()
	defer b.mux.Unlock()

	if pair.IsLessThan(b.smallestSeq) {
		b.smallestSeq = SequencePair{pair.Sequence, pair.SubSequence}
	}
}

func (b *batcher) Flush() {
	b.flushChan <- struct{}{}
}

func (b *batcher) batchSize(batch [][]byte) int {
	total := 0
	for _, msg := range batch {
		total += len(msg)
	}

	return total
}

func (b *batcher) flush(batch [][]byte) [][]byte {
	if len(batch) > 0 {
		b.mux.Lock()
		b.sync.SendBatch(batch)
		b.smallestSeq = SequencePair{nil, 0}
		b.mux.Unlock()
	}
	return [][]byte{}
}

func (b *batcher) startBatcher(msgChan <-chan msgPack, flushChan <-chan struct{}) {
	batch := [][]byte{}

	for {
		select {
		case <-time.After(b.flushInterval):
			batch = b.flush(batch)
		case <-flushChan:
			batch = b.flush(batch)
		case pack := <-msgChan:
			size := b.batchSize(batch)
			if b.flushSize < size+len(pack.msg) {
				batch = b.flush(batch)
			}

			batch = append(batch, pack.msg)
			b.updateSequenceNumbers(pack.sequencePair)

			if b.flushCount <= len(batch) || b.flushSize <= b.batchSize(batch) {
				batch = b.flush(batch)
			}
		}
	}
}
