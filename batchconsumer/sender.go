package batchconsumer

import (
	"errors"
	"fmt"
)

// ErrLogIgnored should be returned by EncodeLog when it encounters a log line that will not be
// consumed
var ErrLogIgnored = errors.New("Log intentionally skipped by sender")

// Sender an interface needed for batch consumer implementations
type Sender interface {
	// EncodeLog receives a raw log line.  It's expected to return an appropriately formated log
	// as well as a list of tags for that log line.  A tag corresponds to a batch that it'll be
	// put into.  Typically tags are series names.
	// If a log line will not be consumed, EncodeLog should return a ErrLogIgnored error.
	EncodeLog(rawlog []byte) (log []byte, tags []string, err error)
	// SendBatch receives a batch of log lines.  All log lines were given the specified tag by
	// EncodeLog
	SendBatch(batch [][]byte, tag string) error
}

// PartialOutputError should be returned by SendBatch implementations when a handful of log lines
// couldn't be sent to an output.  It's expected that SendBatch implementations do a "best effort"
// before returning this error.
type PartialOutputError struct {
	// Message is a description of error that occurred
	Message string
	// Logs a list of logs that failed to be sent
	Logs [][]byte
}

func (c PartialOutputError) Error() string {
	return fmt.Sprintf("%d failed logs. %s", len(c.Logs), c.Message)
}

// CatastrophicOutputError should be returned by SendBatch implementations when the output is
// unreachable.  Returning this error causes this container to exit without checkpointing.
type CatastrophicOutputError struct {
	Message string
}

func (c CatastrophicOutputError) Error() string {
	return c.Message
}
