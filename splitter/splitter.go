package splitter

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"time"
)

// LogEvent is a single log line within a LogEventBatch
type LogEvent struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message"`
}

// LogEventBatch is a batch of multiple log lines, read from a KinesisStream with a CWLogs subscription
type LogEventBatch struct {
	MessageType         string     `json:"messageType"`
	Owner               string     `json:"owner"`
	LogGroup            string     `json:"logGroup"`
	LogStream           string     `json:"logStream"`
	SubscriptionFilters []string   `json:"subscriptionFilters"`
	LogEvents           []LogEvent `json:"logEvents"`
}

// IsGzipped returns whether or not a string is Gzipped (determined by looking for a Gzip byte prefix)
func IsGzipped(b []byte) bool {
	return b[0] == 0x1f && b[1] == 0x8b
}

// GetMessagesFromGzippedInput takes a gzipped record from a CWLogs Subscription and splits it into
// a slice of messages.
func GetMessagesFromGzippedInput(input []byte) ([][]byte, error) {
	unpacked, err := unpack(input)
	if err != nil {
		return [][]byte{}, err
	}
	return Split(unpacked), nil
}

// Unpack expects a gzipped + json-stringified LogEventBatch
func unpack(input []byte) (LogEventBatch, error) {
	gzipReader, err := gzip.NewReader(bytes.NewReader(input))
	if err != nil {
		return LogEventBatch{}, err
	}

	byt, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		return LogEventBatch{}, err
	}

	var dat LogEventBatch
	if err := json.Unmarshal(byt, &dat); err != nil {
		return LogEventBatch{}, err
	}

	return dat, nil
}

// RFC3339Micro is the RFC3339 format in microseconds
const RFC3339Micro = "2006-01-02T15:04:05.999999-07:00"

const taskMeta = `([a-z0-9-]+)--([a-z0-9-]+)\/` + // env--app
	`([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\/` + // task-id
	`([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})` // container-id

var taskRegex = regexp.MustCompile(taskMeta)

// Split takes a LogEventBatch and separates into a slice of enriched log lines
// Lines are enhanced by adding an Rsyslog prefix, which should be handled correctly by
// the subsequent decoding logic.
func Split(b LogEventBatch) [][]byte {
	env := "unknown"
	app := "unknown"
	task := "00001111-2222-3333-4444-555566667777"
	matches := taskRegex.FindAllStringSubmatch(b.LogStream, 1)
	if len(matches) == 1 {
		env = matches[0][1]
		app = matches[0][2]
		task = matches[0][3]
	}

	rsyslogPrefix := `%s %s %s[%d]: %s`
	// programName is a mocked ARN in the format expected by our log decoders
	programName := env + "--" + app + `/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F` + task
	mockPid := 1
	hostname := "aws-batch"

	out := [][]byte{}
	for _, event := range b.LogEvents {
		// Adding an extra Microsecond forces `Format` to include all 6 digits within the micorsecond format.
		// Otherwise, time.Format omits trailing zeroes. (https://github.com/golang/go/issues/12472)
		nsecs := event.Timestamp*int64(time.Millisecond) + int64(time.Microsecond)
		logTime := time.Unix(0, nsecs).UTC().Format(RFC3339Micro)

		// Fake an RSyslog prefix, expected by consumers
		formatted := fmt.Sprintf(rsyslogPrefix, logTime, hostname, programName, mockPid, event.Message)
		out = append(out, []byte(formatted))
	}

	return out
}
