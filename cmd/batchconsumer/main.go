package main

import (
	"fmt"
	"time"

	"gopkg.in/Clever/kayvee-go.v6/logger"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"
)

func main() {
	config := kbc.Config{
		BatchInterval:  10 * time.Second,
		BatchCount:     500,
		BatchSize:      4 * 1024 * 1024, // 4Mb
		FailedLogsFile: "/tmp/example-kcl-consumer",
	}

	sender := &exampleSender{output: logger.New("fake-output")}
	consumer := kbc.NewBatchConsumer(config, sender)
	consumer.Start()
}

type exampleSender struct {
	output logger.KayveeLogger
}

func (e *exampleSender) ProcessMessage(rawmsg []byte) ([]byte, []string, error) {
	if len(rawmsg)%5 == 2 {
		return nil, nil, kbc.ErrMessageIgnored
	}

	tag1 := fmt.Sprintf("tag-%d", len(rawmsg)%5)
	line := tag1 + ": " + string(rawmsg)

	return []byte(line), []string{tag1}, nil
}

func (e *exampleSender) SendBatch(batch [][]byte, tag string) error {
	for idx, line := range batch {
		e.output.InfoD(tag, logger.M{"idx": idx, "line": string(line)})
	}

	return nil
}
