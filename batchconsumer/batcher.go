package batchconsumer

import (
	"fmt"
	"time"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
)

var ErrBatchFull = fmt.Errorf("The batch is full")

type batcher struct {
	flushCount int
	flushSize  int

	Batch       [][]byte
	LastUpdated time.Time
	SmallestSeq kcl.SequencePair
}

func (b *batcher) batchSize(batch [][]byte) int {
	total := 0
	for _, msg := range batch {
		total += len(msg)
	}

	return total
}

func (b *batcher) AddMessage(msg []byte, pair kcl.SequencePair) error {
	if b.flushCount <= len(b.Batch) {
		return ErrBatchFull
	}

	size := b.batchSize(b.Batch)
	if b.flushSize < size+len(msg) {
		return ErrBatchFull
	}

	b.Batch = append(b.Batch, msg)
	if b.SmallestSeq.IsEmpty() || pair.IsLessThan(b.SmallestSeq) {
		b.SmallestSeq = pair
	}
	b.LastUpdated = time.Now()

	return nil
}

func (b *batcher) Clear() {
	b.Batch = [][]byte{}
	b.LastUpdated = time.Time{}
	b.SmallestSeq = kcl.SequencePair{}
}
