package batchconsumer

import (
	"errors"
	"fmt"
)

// ErrMessageIgnored should be returned by ProcessMessage when it encounters a message that will
// not be consumed
var ErrMessageIgnored = errors.New("Message intentionally skipped by sender")

// Sender an interface needed for batch consumer implementations
type Sender interface {
	// Initialize called once before ProcessMessage and SendBatch
	Initialize(shardID string)
	// ProcessMessage receives a raw message and is expected to return an appropriately formatted
	// message as well as a list of tags for that log line.  A tag corresponds to a batch that
	// it'll be put into.  Typically tags are series names.
	// If a message will not be consumed, ProcessMessage should return a ErrMessageIgnored error.
	ProcessMessage(rawmsg []byte) (msg []byte, tags []string, err error)
	// SendBatch receives a batch of messages.  All messages were given the specified tag by
	// ProcessMessage
	SendBatch(batch [][]byte, tag string) error
}

// PartialSendBatchError should be returned by SendBatch implementations when some messages
// couldn't be sent to an output.  It's expected that SendBatch implementations do a "best effort"
// before returning this error.
type PartialSendBatchError struct {
	// ErrMessage is a description of error that occurred
	ErrMessage string
	// FailedMessages a list of messages that failed to be sent
	FailedMessages [][]byte
}

func (c PartialSendBatchError) Error() string {
	return fmt.Sprintf("%d failed logs. %s", len(c.FailedMessages), c.ErrMessage)
}

// CatastrophicSendBatchError should be returned by SendBatch implementations when the output is
// unreachable.  Returning this error causes this container to exit without checkpointing.
type CatastrophicSendBatchError struct {
	ErrMessage string
}

func (c CatastrophicSendBatchError) Error() string {
	return fmt.Sprintf("catastrophic SendBatch error: %s", c.ErrMessage)
}
