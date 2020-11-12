package batchconsumer

import (
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/kayvee-go.v6/logger"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
)

type ignoringSender struct{}

func (i ignoringSender) Initialize(shardID string) {}

func (i ignoringSender) ProcessMessage(rawmsg []byte) (msg []byte, tags []string, err error) {
	return nil, nil, ErrMessageIgnored
}

func (i ignoringSender) SendBatch(batch [][]byte, tag string) error {
	panic("SendBatch Should never be called.  ProcessMessage ignores all messasges.")
}

type tagBatch struct {
	tag   string
	batch [][]byte
}
type msgAsTagSender struct {
	shardID   string
	batches   map[string][][][]byte
	saveBatch chan tagBatch
	shutdown  chan struct{}
}

func NewMsgAsTagSender() *msgAsTagSender {
	sender := &msgAsTagSender{
		batches:   map[string][][][]byte{},
		saveBatch: make(chan tagBatch),
		shutdown:  make(chan struct{}),
	}

	sender.startBatchSaver(sender.saveBatch, sender.shutdown)

	return sender
}

func (i *msgAsTagSender) startBatchSaver(saveBatch <-chan tagBatch, shutdown <-chan struct{}) {
	go func() {
		for {
			select {
			case tb := <-saveBatch:
				batches, ok := i.batches[tb.tag]
				if !ok {
					batches = [][][]byte{}
				}
				i.batches[tb.tag] = append(batches, tb.batch)
			case <-shutdown:
				return
			}
		}
	}()
}

func (i *msgAsTagSender) Initialize(shardID string) {
	i.shardID = shardID
}

func (i *msgAsTagSender) ProcessMessage(rawmsg []byte) (msg []byte, tags []string, err error) {
	if "ignore" == string(rawmsg) {
		return nil, nil, ErrMessageIgnored
	}

	return rawmsg, []string{string(rawmsg)}, nil
}

func (i *msgAsTagSender) SendBatch(batch [][]byte, tag string) error {
	i.saveBatch <- tagBatch{tag, batch}
	return nil
}

func (i *msgAsTagSender) Shutdown() {
	i.shutdown <- struct{}{}
}

type mockCheckpointer struct {
	recievedSequences []string
	checkpoint        chan string
	done              chan struct{}
	timeout           chan struct{}
	shutdown          chan struct{}
}

func NewMockCheckpointer(timeout time.Duration) *mockCheckpointer {
	mcp := &mockCheckpointer{
		checkpoint: make(chan string),
		done:       make(chan struct{}, 1),
		timeout:    make(chan struct{}, 1),
		shutdown:   make(chan struct{}),
	}
	mcp.startWaiter(timeout)

	return mcp
}

func (m *mockCheckpointer) startWaiter(timeout time.Duration) {
	go func() {
		for {
			select {
			case seq := <-m.checkpoint:
				m.recievedSequences = append(m.recievedSequences, seq)
			case <-time.NewTimer(timeout).C:
				m.timeout <- struct{}{}
			case <-m.shutdown:
				m.done <- struct{}{}
				return
			}
		}
	}()
}
func (m *mockCheckpointer) wait() error {
	select {
	case <-m.done:
		return nil
	case <-m.timeout:
		return fmt.Errorf("timeout waiting for checkpoints")
	}
}
func (m *mockCheckpointer) Shutdown() {
	m.shutdown <- struct{}{}
}
func (m *mockCheckpointer) Checkpoint(pair kcl.SequencePair) {
	m.checkpoint <- pair.Sequence.String()
}

func encode(str string) string {
	return base64.StdEncoding.EncodeToString([]byte(str))
}

func TestProcessRecordsIgnoredMessages(t *testing.T) {
	assert := assert.New(t)

	mockFailedLogsFile := logger.New("testing")
	mockconfig := withDefaults(Config{
		BatchInterval:  10 * time.Millisecond,
		CheckpointFreq: 20 * time.Millisecond,
	})
	mockcheckpointer := NewMockCheckpointer(5 * time.Second)

	wrt := NewBatchedWriter(mockconfig, ignoringSender{}, mockFailedLogsFile)
	wrt.Initialize("test-shard", mockcheckpointer)

	err := wrt.ProcessRecords([]kcl.Record{
		kcl.Record{SequenceNumber: "1", Data: encode("hi")},
		kcl.Record{SequenceNumber: "2", Data: encode("hi")},
		kcl.Record{SequenceNumber: "3", Data: encode("hi")},
		kcl.Record{SequenceNumber: "4", Data: encode("hi")},
	})
	assert.NoError(err)

	err = wrt.Shutdown("TERMINATE")
	assert.NoError(err)

	err = mockcheckpointer.wait()
	assert.NoError(err)

	assert.Contains(mockcheckpointer.recievedSequences, "4")
}

func TestProcessRecordsSingleBatchBasic(t *testing.T) {
	assert := assert.New(t)

	mockFailedLogsFile := logger.New("testing")
	mockconfig := withDefaults(Config{
		BatchCount:     2,
		CheckpointFreq: 1, // Don't throttle checks points
	})
	mockcheckpointer := NewMockCheckpointer(5 * time.Second)
	mocksender := NewMsgAsTagSender()

	wrt := NewBatchedWriter(mockconfig, mocksender, mockFailedLogsFile)
	wrt.Initialize("test-shard", mockcheckpointer)

	assert.Equal("test-shard", mocksender.shardID)

	err := wrt.ProcessRecords([]kcl.Record{
		kcl.Record{SequenceNumber: "1", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "2", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "3", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "4", Data: encode("tag1")},
	})
	assert.NoError(err)
	err = wrt.ProcessRecords([]kcl.Record{
		kcl.Record{SequenceNumber: "5", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "6", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "7", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "8", Data: encode("tag1")},
	})
	assert.NoError(err)

	err = wrt.Shutdown("TERMINATE")
	assert.NoError(err)

	err = mockcheckpointer.wait()
	assert.NoError(err)

	mocksender.Shutdown()

	assert.Contains(mocksender.batches, "tag1")
	assert.Equal(4, len(mocksender.batches["tag1"]))

	assert.Contains(mockcheckpointer.recievedSequences, "2")
	assert.Contains(mockcheckpointer.recievedSequences, "4")
	assert.Contains(mockcheckpointer.recievedSequences, "6")
	assert.Contains(mockcheckpointer.recievedSequences, "8")

}

func TestProcessRecordsMutliBatchBasic(t *testing.T) {
	assert := assert.New(t)

	mockFailedLogsFile := logger.New("testing")
	mockconfig := withDefaults(Config{
		BatchInterval:  100 * time.Millisecond,
		CheckpointFreq: 200 * time.Millisecond,
	})
	mockcheckpointer := NewMockCheckpointer(5 * time.Second)
	mocksender := NewMsgAsTagSender()

	wrt := NewBatchedWriter(mockconfig, mocksender, mockFailedLogsFile)
	wrt.Initialize("test-shard", mockcheckpointer)

	err := wrt.ProcessRecords([]kcl.Record{
		kcl.Record{SequenceNumber: "1", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "2", Data: encode("tag2")},
		kcl.Record{SequenceNumber: "3", Data: encode("tag3")},
		kcl.Record{SequenceNumber: "4", Data: encode("tag2")},
	})
	assert.NoError(err)
	err = wrt.ProcessRecords([]kcl.Record{
		kcl.Record{SequenceNumber: "5", Data: encode("tag3")},
		kcl.Record{SequenceNumber: "6", Data: encode("tag2")},
		kcl.Record{SequenceNumber: "7", Data: encode("tag3")},
		kcl.Record{SequenceNumber: "8", Data: encode("tag1")},
	})
	assert.NoError(err)

	err = wrt.Shutdown("TERMINATE")
	assert.NoError(err)

	err = mockcheckpointer.wait()
	assert.NoError(err)

	mocksender.Shutdown()

	assert.Contains(mocksender.batches, "tag1")
	assert.Equal(1, len(mocksender.batches["tag1"]))    // One batch
	assert.Equal(2, len(mocksender.batches["tag1"][0])) // with two items
	assert.Equal("tag1", string(mocksender.batches["tag1"][0][0]))
	assert.Equal("tag1", string(mocksender.batches["tag1"][0][1]))

	assert.Contains(mocksender.batches, "tag2")
	assert.Equal(1, len(mocksender.batches["tag2"]))    // One batch
	assert.Equal(3, len(mocksender.batches["tag2"][0])) // with three items
	assert.Equal("tag2", string(mocksender.batches["tag2"][0][0]))
	assert.Equal("tag2", string(mocksender.batches["tag2"][0][1]))
	assert.Equal("tag2", string(mocksender.batches["tag2"][0][2]))

	assert.Contains(mocksender.batches, "tag3")
	assert.Equal(1, len(mocksender.batches["tag3"]))    // One batch
	assert.Equal(3, len(mocksender.batches["tag3"][0])) // with three items
	assert.Equal("tag3", string(mocksender.batches["tag3"][0][0]))
	assert.Equal("tag3", string(mocksender.batches["tag3"][0][1]))
	assert.Equal("tag3", string(mocksender.batches["tag3"][0][2]))
}

func TestProcessRecordsMutliBatchWithIgnores(t *testing.T) {
	assert := assert.New(t)

	mockFailedLogsFile := logger.New("testing")
	mockconfig := withDefaults(Config{
		BatchInterval:  100 * time.Millisecond,
		CheckpointFreq: 200 * time.Millisecond,
	})
	mockcheckpointer := NewMockCheckpointer(5 * time.Second)
	mocksender := NewMsgAsTagSender()

	wrt := NewBatchedWriter(mockconfig, mocksender, mockFailedLogsFile)
	wrt.Initialize("test-shard", mockcheckpointer)

	err := wrt.ProcessRecords([]kcl.Record{
		kcl.Record{SequenceNumber: "1", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "2", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "3", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "4", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "5", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "6", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "7", Data: encode("tag2")},
		kcl.Record{SequenceNumber: "8", Data: encode("tag3")},
		kcl.Record{SequenceNumber: "9", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "10", Data: encode("tag2")},
		kcl.Record{SequenceNumber: "11", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "12", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "13", Data: encode("ignore")},
	})
	assert.NoError(err)
	err = wrt.ProcessRecords([]kcl.Record{
		kcl.Record{SequenceNumber: "14", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "15", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "16", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "17", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "18", Data: encode("tag3")},
		kcl.Record{SequenceNumber: "19", Data: encode("tag2")},
		kcl.Record{SequenceNumber: "20", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "21", Data: encode("tag3")},
		kcl.Record{SequenceNumber: "22", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "23", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "24", Data: encode("ignore")},
		kcl.Record{SequenceNumber: "25", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "26", Data: encode("ignore")},
	})
	assert.NoError(err)

	err = wrt.Shutdown("TERMINATE")
	assert.NoError(err)

	err = mockcheckpointer.wait()
	assert.NoError(err)

	mocksender.Shutdown()

	assert.Contains(mocksender.batches, "tag1")
	assert.Equal(1, len(mocksender.batches["tag1"]))    // One batch
	assert.Equal(2, len(mocksender.batches["tag1"][0])) // with two items
	assert.Equal("tag1", string(mocksender.batches["tag1"][0][0]))
	assert.Equal("tag1", string(mocksender.batches["tag1"][0][1]))

	assert.Contains(mocksender.batches, "tag2")
	assert.Equal(1, len(mocksender.batches["tag2"]))    // One batch
	assert.Equal(3, len(mocksender.batches["tag2"][0])) // with three items
	assert.Equal("tag2", string(mocksender.batches["tag2"][0][0]))
	assert.Equal("tag2", string(mocksender.batches["tag2"][0][1]))
	assert.Equal("tag2", string(mocksender.batches["tag2"][0][2]))

	assert.Contains(mocksender.batches, "tag3")
	assert.Equal(1, len(mocksender.batches["tag3"]))    // One batch
	assert.Equal(3, len(mocksender.batches["tag3"][0])) // with three items
	assert.Equal("tag3", string(mocksender.batches["tag3"][0][0]))
	assert.Equal("tag3", string(mocksender.batches["tag3"][0][1]))
	assert.Equal("tag3", string(mocksender.batches["tag3"][0][2]))
}

func TestStaggeredCheckpointing(t *testing.T) {
	assert := assert.New(t)

	mockFailedLogsFile := logger.New("testing")
	mockconfig := withDefaults(Config{
		BatchCount:     2,
		BatchInterval:  100 * time.Millisecond,
		CheckpointFreq: 200 * time.Nanosecond,
	})
	mockcheckpointer := NewMockCheckpointer(5 * time.Second)
	mocksender := NewMsgAsTagSender()

	wrt := NewBatchedWriter(mockconfig, mocksender, mockFailedLogsFile)
	wrt.Initialize("test-shard", mockcheckpointer)

	err := wrt.ProcessRecords([]kcl.Record{
		kcl.Record{SequenceNumber: "1", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "2", Data: encode("tag3")},
		kcl.Record{SequenceNumber: "3", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "4", Data: encode("tag3")},
	})
	assert.NoError(err)
	err = wrt.ProcessRecords([]kcl.Record{
		kcl.Record{SequenceNumber: "5", Data: encode("tag1")},
		kcl.Record{SequenceNumber: "6", Data: encode("tag3")},
		kcl.Record{SequenceNumber: "7", Data: encode("tag3")},
		kcl.Record{SequenceNumber: "8", Data: encode("tag3")},
		kcl.Record{SequenceNumber: "9", Data: encode("tag3")},
	})
	assert.NoError(err)

	time.Sleep(200 * time.Millisecond) // Sleep to ensure checkpoint get flushed at least once

	err = wrt.Shutdown("TERMINATE")
	assert.NoError(err)

	err = mockcheckpointer.wait()
	assert.NoError(err)

	mocksender.Shutdown()

	// Test to make sure writer doesn't prematurely checkpoint messages
	// Checkpoints 5,6,7,8 will never be submitted because the 3rd "tag1" is in a batch
	// Checkpoint 9 is submitted on shutdown when everything is being flushed
	assert.NotContains(mockcheckpointer.recievedSequences, "5")
	assert.NotContains(mockcheckpointer.recievedSequences, "6")
	assert.NotContains(mockcheckpointer.recievedSequences, "7")
	assert.NotContains(mockcheckpointer.recievedSequences, "8")
	assert.Contains(mockcheckpointer.recievedSequences, "9")

	assert.Contains(mocksender.batches, "tag1")
	assert.Equal(2, len(mocksender.batches["tag1"]))    // One batch
	assert.Equal(2, len(mocksender.batches["tag1"][0])) // with two items
	assert.Equal("tag1", string(mocksender.batches["tag1"][0][0]))
	assert.Equal("tag1", string(mocksender.batches["tag1"][0][1]))
	assert.Equal("tag1", string(mocksender.batches["tag1"][1][0]))

	assert.Contains(mocksender.batches, "tag3")
	assert.Equal(3, len(mocksender.batches["tag3"]))    // One batch
	assert.Equal(2, len(mocksender.batches["tag3"][0])) // with three items
	assert.Equal("tag3", string(mocksender.batches["tag3"][0][0]))
	assert.Equal("tag3", string(mocksender.batches["tag3"][0][1]))
	assert.Equal(2, len(mocksender.batches["tag3"][1]))
	assert.Equal("tag3", string(mocksender.batches["tag3"][1][0]))
	assert.Equal("tag3", string(mocksender.batches["tag3"][1][1]))
	assert.Equal(2, len(mocksender.batches["tag3"][2]))
	assert.Equal("tag3", string(mocksender.batches["tag3"][2][0]))
	assert.Equal("tag3", string(mocksender.batches["tag3"][2][1]))
}
