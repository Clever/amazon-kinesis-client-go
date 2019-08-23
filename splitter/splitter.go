package splitter

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"time"
)

// LogEvent is a single log line within a LogEventBatch
type LogEvent struct {
	ID        string              `json:"id"`
	Timestamp UnixTimestampMillis `json:"timestamp"`
	Message   string              `json:"message"`
}

// UnixTimestampMillis is a time.Time that marshals (unmarshals) to (from) a unix timestamp with millisecond resolution.
type UnixTimestampMillis time.Time

func NewUnixTimestampMillis(ts int64) UnixTimestampMillis {
	return UnixTimestampMillis(time.Unix(ts/millisPerSecond,
		(ts%millisPerSecond)*nanosPerMillisecond))
}

func (t *UnixTimestampMillis) MarshalJSON() ([]byte, error) {
	ts := time.Time(*t).UnixNano()
	stamp := fmt.Sprint(ts / nanosPerMillisecond)

	return []byte(stamp), nil
}

var millisPerSecond = int64(time.Second / time.Millisecond)
var nanosPerMillisecond = int64(time.Millisecond / time.Nanosecond)

func (t *UnixTimestampMillis) UnmarshalJSON(b []byte) error {
	ts, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*t = NewUnixTimestampMillis(ts)
	return nil
}

func (t *UnixTimestampMillis) Time() time.Time {
	return time.Time(*t)
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

// http://docs.aws.amazon.com/batch/latest/userguide/job_states.html
// "log stream name format is jobDefinitionName/default/ecs_task_id (this format may change in the future)."
const awsBatchTaskMeta = `([a-z0-9-]+)--([a-z0-9-]+)\/` + // env--app
	`default\/` +
	`([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})` // task-id
var awsBatchTaskRegex = regexp.MustCompile(awsBatchTaskMeta)

// lambda log groups are of the form /aws/lambda/<env>--<app>
var awsLambdaLogGroupRegex = regexp.MustCompile(`^/aws/lambda/([a-z0-9-]+)--([a-z0-9-]+)$`)
var awsLambdaRequestIDRegex = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// fargate log groups are of the form /ecs/<env>--<app>
// fargate log streams are of the form fargate/<container name>/<ecs task id>
var awsFargateLogGroupRegex = regexp.MustCompile(`^/ecs/([a-z0-9-]+)--([a-z0-9-]+)$`)
var awsFargateLogStreamRegex = regexp.MustCompile(`^fargate/([a-z0-9-]+)--([a-z0-9-]+)/([a-z0-9]+)$`)

// RDS slowquery log groups are in the form of /aws/rds/cluster/<database name>/slowquery
var awsRDSLogGroupRegex = regexp.MustCompile(`^/aws/rds/cluster/([a-z0-9-]+)/slowquery$`)

// arn and task cruft to satisfy parsing later on: https://github.com/Clever/amazon-kinesis-client-go/blob/94aacdf8339bd2cc8400d3bcb323dc1bce2c8422/decode/decode.go#L421-L425
const arnCruft = `/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F`
const taskCruft = `12345678-1234-1234-1234-555566667777`

type RSysLogMessage struct {
	Timestamp   time.Time
	Hostname    string
	ProgramName string
	Message     string
}

func (r RSysLogMessage) String() string {
	// Adding an extra Microsecond forces `Format` to include all 6 digits within the micorsecond format.
	// Otherwise, time.Format omits trailing zeroes. (https://github.com/golang/go/issues/12472)
	return fmt.Sprintf(`%s %s %s: %s`,
		r.Timestamp.Add(time.Microsecond).Format(RFC3339Micro),
		r.Hostname, r.ProgramName, r.Message)
}

func splitAWSBatch(b LogEventBatch) []RSysLogMessage {
	matches := awsBatchTaskRegex.FindAllStringSubmatch(b.LogStream, 1)
	if len(matches) != 1 {
		return nil
	}
	env := matches[0][1]
	app := matches[0][2]
	task := matches[0][3]

	out := []RSysLogMessage{}
	for _, event := range b.LogEvents {
		out = append(out, RSysLogMessage{
			Timestamp:   event.Timestamp.Time(),
			ProgramName: env + "--" + app + arnCruft + task,
			Hostname:    "aws-batch",
			Message:     event.Message,
		})
	}
	return out
}

func splitAWSLambda(b LogEventBatch) []RSysLogMessage {
	matches := awsLambdaLogGroupRegex.FindAllStringSubmatch(b.LogGroup, 1)
	if len(matches) != 1 {
		return nil
	}
	env := matches[0][1]
	app := matches[0][2]

	out := []RSysLogMessage{}
	for _, event := range b.LogEvents {
		// find the request ID, e.g. 1f7fcc25-015f-11e8-a728-a1b6168ab9aa, set it as task
		var task string
		if matches := awsLambdaRequestIDRegex.FindAllString(event.Message, 1); len(matches) == 1 {
			task = matches[0]
		} else {
			task = taskCruft // rsyslog message must contain a non-empty task ID to satisfy later parsing
		}
		out = append(out, RSysLogMessage{
			Timestamp:   event.Timestamp.Time(),
			ProgramName: env + "--" + app + arnCruft + task,
			Hostname:    "aws-lambda",
			Message:     event.Message,
		})
	}
	return out
}

func splitAWSFargate(b LogEventBatch) []RSysLogMessage {
	matches := awsFargateLogGroupRegex.FindAllStringSubmatch(b.LogGroup, 1)
	if len(matches) != 1 {
		return nil
	}
	env := matches[0][1]
	app := matches[0][2]

	streamMatches := awsFargateLogStreamRegex.FindAllStringSubmatch(b.LogStream, 1)
	if len(streamMatches) != 1 {
		return nil
	}
	ecsTaskID := streamMatches[0][3]

	out := []RSysLogMessage{}
	for _, event := range b.LogEvents {
		out = append(out, RSysLogMessage{
			Timestamp:   event.Timestamp.Time(),
			ProgramName: env + "--" + app + arnCruft + ecsTaskID,
			Hostname:    "aws-fargate",
			Message:     event.Message,
		})
	}
	return out
}

func splitAWSRDS(b LogEventBatch) []RSysLogMessage {
	matches := awsRDSLogGroupRegex.FindAllStringSubmatch(b.LogGroup, 1)
	if len(matches) != 1 {
		return nil
	}
	databaseName := matches[0][1]

	out := []RSysLogMessage{}
	for _, event := range b.LogEvents {
		out = append(out, RSysLogMessage{
			Timestamp:   event.Timestamp.Time(),
			ProgramName: databaseName,
			PID:         1,
			Hostname:    "aws-rds",
			Message:     event.Message,
		})
	}
	return out
}

func splitDefault(b LogEventBatch) []RSysLogMessage {
	out := []RSysLogMessage{}
	for _, event := range b.LogEvents {
		out = append(out, RSysLogMessage{
			Timestamp:   event.Timestamp.Time(),
			Hostname:    b.LogStream,
			ProgramName: b.LogGroup + "--" + b.LogStream + arnCruft + taskCruft,
			Message:     event.Message,
		})
	}
	return out
}

// Split takes a LogEventBatch and separates into a slice of enriched log lines
// Lines are enhanced by adding an Rsyslog prefix, which should be handled correctly by
// the subsequent decoding logic.
func Split(b LogEventBatch) [][]byte {
	var rsyslogMsgs []RSysLogMessage

	if awsLambdaLogGroupRegex.MatchString(b.LogGroup) {
		rsyslogMsgs = splitAWSLambda(b)
	} else if awsFargateLogGroupRegex.MatchString(b.LogGroup) {
		rsyslogMsgs = splitAWSFargate(b)
	} else if awsBatchTaskRegex.MatchString(b.LogStream) {
		rsyslogMsgs = splitAWSBatch(b)
	} else if awsRDSLogGroupRegex.MatchString(b.LogGroup) {
		rsyslogMsgs = splitAWSRDS(b)
	} else {
		rsyslogMsgs = splitDefault(b)
	}

	out := [][]byte{}
	for _, rsyslogMsg := range rsyslogMsgs {
		out = append(out, []byte(rsyslogMsg.String()))
	}

	return out
}
