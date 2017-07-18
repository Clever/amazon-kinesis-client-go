package batchconsumer

import (
	"errors"
	"fmt"
)

var ErrLogIgnored = errors.New("Log intentionally skipped by sender")

type Sender interface {
	EncodeLog(rawlog []byte) (log []byte, tags []string, err error)
	SendBatch(batch [][]byte, tag string) error
}

type PartialOutputError struct {
	Message string
	Logs    [][]byte
}

func (c PartialOutputError) Error() string {
	return fmt.Sprintf("%d failed logs. %s", len(c.Logs), c.Message)
}

type CatastrophicOutputError struct {
	Message string
}

func (c CatastrophicOutputError) Error() string {
	return c.Message
}
