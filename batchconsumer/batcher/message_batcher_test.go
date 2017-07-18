package batcher

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type batch [][]byte

type MockSync struct {
	flushChan chan struct{}
	batches   []batch
}

func NewMockSync() *MockSync {
	return &MockSync{
		flushChan: make(chan struct{}, 1),
		batches:   []batch{},
	}
}

func (m *MockSync) SendBatch(b [][]byte) {
	m.batches = append(m.batches, batch(b))
	m.flushChan <- struct{}{}
}

func (m *MockSync) waitForFlush(timeout time.Duration) error {
	select {
	case <-m.flushChan:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timed out before flush (waited %s)", timeout.String())
	}
}

const mockSequenceNumber = "99999"
const mockSubSequenceNumber = 12345

func TestBatchingByCount(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync, time.Hour, 2, 1024*1024)

	t.Log("Batcher respect count limit")
	assert.NoError(batcher.AddMessage([]byte("hihi"), mockSequenceNumber, mockSubSequenceNumber))
	assert.NoError(batcher.AddMessage([]byte("heyhey"), mockSequenceNumber, mockSubSequenceNumber))
	assert.NoError(batcher.AddMessage([]byte("hmmhmm"), mockSequenceNumber, mockSubSequenceNumber))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(2, len(sync.batches[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))
	assert.Equal("heyhey", string(sync.batches[0][1]))

	t.Log("Batcher doesn't send partial batches")
	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)
}

func TestBatchingByTime(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync, time.Millisecond, 2000000, 1024*1024)

	t.Log("Batcher sends partial batches when time expires")
	assert.NoError(batcher.AddMessage([]byte("hihi"), mockSequenceNumber, mockSubSequenceNumber))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))

	t.Log("Batcher sends all messsages in partial batches when time expires")
	assert.NoError(batcher.AddMessage([]byte("heyhey"), mockSequenceNumber, mockSubSequenceNumber))
	assert.NoError(batcher.AddMessage([]byte("yoyo"), mockSequenceNumber, mockSubSequenceNumber))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(2, len(sync.batches))
	assert.Equal(2, len(sync.batches[1]))
	assert.Equal("heyhey", string(sync.batches[1][0]))
	assert.Equal("yoyo", string(sync.batches[1][1]))

	t.Log("Batcher doesn't send empty batches")
	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)
}

func TestBatchingBySize(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync, time.Hour, 2000000, 8)

	t.Log("Large messages are sent immediately")
	assert.NoError(batcher.AddMessage([]byte("hellohello"), mockSequenceNumber, mockSubSequenceNumber))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal("hellohello", string(sync.batches[0][0]))

	t.Log("Batcher tries not to exceed size limit")
	assert.NoError(batcher.AddMessage([]byte("heyhey"), mockSequenceNumber, mockSubSequenceNumber))
	assert.NoError(batcher.AddMessage([]byte("hihi"), mockSequenceNumber, mockSubSequenceNumber))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(2, len(sync.batches))
	assert.Equal(1, len(sync.batches[1]))
	assert.Equal("heyhey", string(sync.batches[1][0]))

	t.Log("Batcher sends messages that didn't fit in previous batch")
	assert.NoError(batcher.AddMessage([]byte("yoyo"), mockSequenceNumber, mockSubSequenceNumber)) // At this point "hihi" is in the batch

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(3, len(sync.batches))
	assert.Equal(2, len(sync.batches[2]))
	assert.Equal("hihi", string(sync.batches[2][0]))
	assert.Equal("yoyo", string(sync.batches[2][1]))

	t.Log("Batcher doesn't send partial batches")
	assert.NoError(batcher.AddMessage([]byte("okok"), mockSequenceNumber, mockSubSequenceNumber))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)
}

func TestFlushing(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync, time.Hour, 2000000, 1024*1024)

	t.Log("Calling flush sends pending messages")
	assert.NoError(batcher.AddMessage([]byte("hihi"), mockSequenceNumber, mockSubSequenceNumber))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)

	batcher.Flush()

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))
}

func TestSendingEmpty(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync, time.Second, 10, 1024*1024)

	t.Log("An error is returned when an empty message is sent")
	err = batcher.AddMessage([]byte{}, mockSequenceNumber, mockSubSequenceNumber)
	assert.Error(err)
}

func TestUpdatingSequence(t *testing.T) {
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync, time.Second, 10, 1024*1024).(*batcher)

	t.Log("Initally, smallestSeq is undefined")
	expected := new(big.Int)
	assert.Nil(batcher.smallestSeq.Sequence)

	t.Log("After AddMessage (seq=1), smallestSeq = 1")
	assert.NoError(batcher.AddMessage([]byte("abab"), "1", mockSubSequenceNumber))
	sync.waitForFlush(time.Minute)
	expected.SetInt64(1)
	seq := batcher.SmallestSequencePair()
	assert.True(expected.Cmp(seq.Sequence) == 0)

	t.Log("After AddMessage (seq=2), smallestSeq = 1 -- not updated because higher")
	assert.NoError(batcher.AddMessage([]byte("cdcd"), "2", mockSubSequenceNumber))
	sync.waitForFlush(time.Minute)
	seq = batcher.SmallestSequencePair()
	assert.True(expected.Cmp(seq.Sequence) == 0)

	t.Log("After AddMessage (seq=1), smallestSeq = 0")
	assert.NoError(batcher.AddMessage([]byte("efef"), "0", mockSubSequenceNumber))
	sync.waitForFlush(time.Minute)
	expected.SetInt64(0)
	seq = batcher.SmallestSequencePair()
	assert.True(expected.Cmp(seq.Sequence) == 0)
}
