package batchconsumer

import (
	"io"
	"log"
	"os"
	"time"

	"gopkg.in/Clever/kayvee-go.v6/logger"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
)

// Config used for BatchConsumer constructor.  Any empty fields are populated with defaults.
type Config struct {
	// LogFile where consumer errors and failed log lines are saved
	LogFile string

	// BatchInterval the upper bound on how often SendBatch is called with accumulated messages
	BatchInterval time.Duration
	// BatchCount is the number of messages that triggers a SendBatch call
	BatchCount int
	// BatchSize is the size of a batch in bytes that triggers a SendBatch call
	BatchSize int

	// ReadRateLimit the number of records read per seconds
	ReadRateLimit int
	// ReadBurstLimit the max number of tokens allowed by rate limiter
	ReadBurstLimit int

	// CheckpointFreq the frequency in which a checkpoint is saved
	CheckpointFreq time.Duration
}

// BatchConsumer is responsible for marshalling
type BatchConsumer struct {
	kclProcess *kcl.KCLProcess
	logfile    *os.File
}

func withDefaults(config Config) Config {
	if config.LogFile == "" {
		config.LogFile = "/tmp/kcl-" + time.Now().Format(time.RFC3339)
	}

	if config.BatchInterval == 0 {
		config.BatchInterval = 10 * time.Second
	}
	if config.BatchCount == 0 {
		config.BatchCount = 500
	}
	if config.BatchSize == 0 {
		config.BatchSize = 4 * 1024 * 1024
	}

	// Not totally clear we need this rate limit.  The KCL may do rate limiting for us.
	if config.ReadRateLimit == 0 {
		config.ReadRateLimit = 1000
	}
	if config.ReadBurstLimit == 0 {
		config.ReadBurstLimit = int(float64(config.ReadRateLimit)*1.2 + 0.5)
	}

	if config.CheckpointFreq == 0 {
		config.CheckpointFreq = 60 * time.Second
	}

	return config
}

// NewBatchConsumerFromFiles creates a batch consumer.  Readers/writers provided are used for
// interprocess communication.
func NewBatchConsumerFromFiles(
	config Config, sender Sender, input io.Reader, output, errFile io.Writer,
) *BatchConsumer {
	config = withDefaults(config)

	file, err := os.OpenFile(config.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Unable to create log file: %s", err.Error())
	}

	kvlog := logger.New("amazon-kinesis-client-go")
	kvlog.SetOutput(file)

	wrt := NewBatchedWriter(config, sender, kvlog)
	kclProcess := kcl.New(input, output, errFile, wrt)

	return &BatchConsumer{
		kclProcess: kclProcess,
		logfile:    file,
	}
}

// NewBatchConsumer creates batch consumer.  Stdin, Stdout, and Stderr are used for interprocess
// communication.
func NewBatchConsumer(config Config, sender Sender) *BatchConsumer {
	return NewBatchConsumerFromFiles(config, sender, os.Stdin, os.Stdout, os.Stderr)
}

// Start when called, the consumer begins ingesting messages.  This function blocks.
func (b *BatchConsumer) Start() {
	b.kclProcess.Run()
	b.logfile.Close()
}
