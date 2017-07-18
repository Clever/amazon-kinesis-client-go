package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"gopkg.in/Clever/kayvee-go.v6/logger"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"
)

func createDummyOutput() (logger.KayveeLogger, *os.File) {
	file, err := os.OpenFile("/tmp/example-kcl-output", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Unable to create log file: %s", err.Error())
	}

	kvlog := logger.New("amazon-kinesis-client-go")
	kvlog.SetOutput(file)

	return kvlog, file
}

func main() {
	config := kbc.Config{
		FlushInterval: 10 * time.Second,
		FlushCount:    500,
		FlushSize:     4 * 1024 * 1024, // 4Mb
		LogFile:       "/tmp/example-kcl-consumer",
		DeployEnv:     "test-env",
	}

	output, file := createDummyOutput()
	defer file.Close()

	sender := &exampleSender{output: output}
	consumer := kbc.NewBatchConsumer(config, sender)
	consumer.Start()
}

type exampleSender struct {
	output logger.KayveeLogger
}

func (e *exampleSender) EncodeLog(rawlog []byte) ([]byte, []string, error) {
	if len(rawlog)%5 == 2 {
		return nil, nil, kbc.ErrLogIgnored
	}

	tag1 := fmt.Sprintf("tag-%d", len(rawlog)%5)
	line := tag1 + ": " + string(rawlog)

	return []byte(line), []string{tag1}, nil
}

func (e *exampleSender) SendBatch(batch [][]byte, tag string) error {
	for idx, line := range batch {
		e.output.InfoD(tag, logger.M{"idx": idx, "line": string(line)})
	}

	return nil
}
