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
	// DeployEnv the name of the deployment environment
	DeployEnv string

	// LogFile where consumer errors and failed log lines are saved
	LogFile string
	// FlushInterval is how often accumulated messages should be bulk put to firehose
	FlushInterval time.Duration
	// FlushCount is the number of messages that triggers a push to firehose. Max batch size is 500, see: http://docs.aws.amazon.com/firehose/latest/dev/limits.html
	FlushCount int
	// FlushSize is the size of a batch in bytes that triggers a push to firehose. Max batch size is 4Mb (4*1024*1024), see: http://docs.aws.amazon.com/firehose/latest/dev/limits.html
	FlushSize int

	// ReadRateLimit the number of records read per seconds
	ReadRateLimit int
	// ReadBurstLimit the max number of tokens allowed by rate limiter
	ReadBurstLimit int

	// CheckpointFreq the frequency in which a checkpoint is saved
	CheckpointFreq time.Duration
	// CheckpointRetries the number of times the consumer will try to save a checkpoint
	CheckpointRetries int
	// CheckpointRetrySleep the amount of time between checkpoint save attempts
	CheckpointRetrySleep time.Duration
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

	if config.FlushInterval == 0 {
		config.FlushInterval = 10 * time.Second
	}
	if config.FlushCount == 0 {
		config.FlushCount = 500
	}
	if config.FlushSize == 0 {
		config.FlushSize = 4 * 1024 * 1024
	}

	if config.DeployEnv == "" {
		config.DeployEnv = "unknown-env"
	}

	if config.ReadRateLimit == 0 {
		config.ReadRateLimit = 300
	}
	if config.ReadBurstLimit == 0 {
		config.ReadBurstLimit = int(float64(config.ReadRateLimit)*1.2 + 0.5)
	}

	if config.CheckpointFreq == 0 {
		config.CheckpointFreq = 60 * time.Second
	}
	if config.CheckpointRetries == 0 {
		config.CheckpointRetries = 5
	}
	if config.CheckpointRetrySleep == 0 {
		config.CheckpointRetrySleep = 5 * time.Second
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

	wrt := &batchedWriter{
		config: config,
		log:    kvlog,
		sender: sender,
	}
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
