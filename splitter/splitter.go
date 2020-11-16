// Package splitter provides functions for decoding various kinds of records that might come off of a kinesis stream.
// It is equipped to with the functions to unbundle KPL aggregates and CloudWatch log bundles,
// as well as apply appropriate decompression.
// KCL applications would be most interested in `SplitMessageIfNecessary` which can handle zlibbed records as well as
// CloudWatch bundles. KCL automatically unbundles KPL aggregates before passing the records to the consumer.
// Non-KCL applications (such as Lambdas consuming KPL-produced aggregates) should either use
// - KPLDeaggregate if the consumer purely wants to unbundle KPL aggregates, but will handle the raw records themselves.
// - Deaggregate if the consumer wants to apply the same decompress and split logic as SplitMessageIfNecessary
//   in addition to the KPL splitting.
package splitter

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"time"

	kpl "github.com/a8m/kinesis-producer"
	"github.com/golang/protobuf/proto"
)

// The Amazon Kinesis Producer Library (KPL) aggregates multiple logical user records into a single
// Amazon Kinesis record for efficient puts.
// https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
var kplMagicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}

// IsKPLAggregate checks a record for a KPL aggregate prefix.
// It is not necessary to call this before calling KPLDeaggregate.
func IsKPLAggregate(data []byte) bool {
	return bytes.HasPrefix(data, kplMagicNumber)
}

// KPLDeaggregate takes a Kinesis record and converts it to one or more user records by applying KPL deaggregation.
// If the record begins with the 4-byte magic prefix that KPL uses, the single Kinesis record is split into its component user records.
// Otherwise, the return value is a singleton containing the original record.
func KPLDeaggregate(kinesisRecord []byte) ([][]byte, error) {
	if !IsKPLAggregate(kinesisRecord) {
		return [][]byte{kinesisRecord}, nil
	}
	src := kinesisRecord[len(kplMagicNumber) : len(kinesisRecord)-md5.Size]
	checksum := kinesisRecord[len(kinesisRecord)-md5.Size:]
	recordSum := md5.Sum(src)
	for i, b := range checksum {
		if b != recordSum[i] {
			// either the data is corrupted or this is not a KPL aggregate
			// either way, return the data as-is
			return [][]byte{kinesisRecord}, nil
		}
	}
	dest := new(kpl.AggregatedRecord)
	err := proto.Unmarshal(src, dest)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling proto: %v", err)
	}
	var records [][]byte
	for _, userRecord := range dest.GetRecords() {
		records = append(records, userRecord.Data)
	}
	return records, nil
}

// Deaggregate is a combination of KPLDeaggregate and SplitMessageIfNecessary
// First it tries to KPL-deaggregate. If unsuccessful, it calls SplitIfNecessary on the original record.
// If successful, it iterates over the individual user records and attempts to unzlib them.
// If a record inside an aggregate is in zlib format, the output will contain the unzlibbed version.
// If it is not zlibbed, the output will contain the record verbatim
// A similar result can be optained by calling KPLDeaggregate, then iterating over the results and callin SplitMessageIfNecessary.
// This function makes the assumption that after KPL-deaggregating, the results are not CloudWatch aggregates, so it doesn't need to check them for a gzip header.
// Also it lets us iterate over the user records one less time, since KPLDeaggregate loops over the records and we would need to loop again to unzlib.
//
// See the SplitMessageIfNecessary documentation for the format of output for CloudWatch log bundles.
func Deaggregate(kinesisRecord []byte) ([][]byte, error) {
	if !IsKPLAggregate(kinesisRecord) {
		return SplitMessageIfNecessary(kinesisRecord)
	}
	src := kinesisRecord[len(kplMagicNumber) : len(kinesisRecord)-md5.Size]
	checksum := kinesisRecord[len(kinesisRecord)-md5.Size:]
	recordSum := md5.Sum(src)
	for i, b := range checksum {
		if b != recordSum[i] {
			// either the data is corrupted or this is not a KPL aggregate
			// either way, return the data as-is
			return [][]byte{kinesisRecord}, nil
		}
	}
	dest := new(kpl.AggregatedRecord)
	err := proto.Unmarshal(src, dest)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling proto: %v", err)
	}
	var records [][]byte
	for _, userRecord := range dest.GetRecords() {
		record, err := unzlib(userRecord.Data)
		if err != nil {
			return nil, fmt.Errorf("unzlibbing record: %w", err)
		}
		records = append(records, record)
	}
	return records, nil
}

// SplitMessageIfNecessary recieves a user-record and returns a slice of one or more records.
// if the record is coming off of a kinesis stream and might be KPL aggregated, it needs to be deaggregated before calling this.
// This function handles three types of records:
// - records emitted from CWLogs Subscription (which are gzip compressed)
// - zlib compressed records (e.g. as compressed and emitted by Kinesis plugin for Fluent Bit
// - any other record (left unchanged)
//
// CloudWatch logs come as structured JSON. In the process of splitting, they are converted
// into an rsyslog format that allows fairly uniform parsing of the result across the
// AWS services that might emit logs to CloudWatch.
// Note that these timezone used in these syslog records is guessed based on the local env.
// If you need consistent timezones, set TZ=UTC in your environment.
func SplitMessageIfNecessary(userRecord []byte) ([][]byte, error) {
	// First try the record as a CWLogs record
	if IsGzipped(userRecord) {
		return GetMessagesFromGzippedInput(userRecord)
	}

	unzlibRecord, err := unzlib(userRecord)
	if err != nil {
		return nil, err
	}

	// Process a single message, from KPL
	return [][]byte{unzlibRecord}, nil
}

func unzlib(input []byte) ([]byte, error) {
	zlibReader, err := zlib.NewReader(bytes.NewReader(input))
	if err == nil {
		unzlibRecord, err := ioutil.ReadAll(zlibReader)
		if err != nil {
			return nil, fmt.Errorf("reading zlib-compressed record: %v", err)
		}
		return unzlibRecord, nil
	}
	return input, nil

}

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

// glue log groups are of the form /aws-glue/jobs/<env>/<app>/<pod-id>
// glue log streams are of the form <job id>-<"driver" | "1" | "progress-bar">
var awsGlueLogGroupRegex = regexp.MustCompile(`^/aws-glue/jobs/([a-z0-9-]+)/([a-z0-9-]+)/([a-z0-9-]+)$`)
var awsGlueLogStreamRegex = regexp.MustCompile(`^(jr_[a-z0-9-]+)-.*$`)

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

func splitAWSBatch(b LogEventBatch) ([]RSysLogMessage, bool) {
	matches := awsBatchTaskRegex.FindAllStringSubmatch(b.LogStream, 1)
	if len(matches) != 1 {
		return nil, false
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
	return out, true
}

func splitAWSLambda(b LogEventBatch) ([]RSysLogMessage, bool) {
	matches := awsLambdaLogGroupRegex.FindAllStringSubmatch(b.LogGroup, 1)
	if len(matches) != 1 {
		return nil, false
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
	return out, true
}

func splitAWSFargate(b LogEventBatch) ([]RSysLogMessage, bool) {
	matches := awsFargateLogGroupRegex.FindAllStringSubmatch(b.LogGroup, 1)
	if len(matches) != 1 {
		return nil, false
	}
	env := matches[0][1]
	app := matches[0][2]

	streamMatches := awsFargateLogStreamRegex.FindAllStringSubmatch(b.LogStream, 1)
	if len(streamMatches) != 1 {
		return nil, false
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
	return out, true
}

func splitAWSRDS(b LogEventBatch) ([]RSysLogMessage, bool) {
	matches := awsRDSLogGroupRegex.FindAllStringSubmatch(b.LogGroup, 1)
	if len(matches) != 1 {
		return nil, false
	}
	databaseName := matches[0][1]

	out := []RSysLogMessage{}
	for _, event := range b.LogEvents {
		out = append(out, RSysLogMessage{
			Timestamp:   event.Timestamp.Time(),
			ProgramName: databaseName,
			Hostname:    "aws-rds",
			Message:     event.Message,
		})
	}
	return out, true
}

func splitAWSGlue(b LogEventBatch) ([]RSysLogMessage, bool) {
	matches := awsGlueLogGroupRegex.FindAllStringSubmatch(b.LogGroup, 1)
	if len(matches) != 1 {
		return nil, false
	}
	env := matches[0][1]
	app := matches[0][2]

	streamMatches := awsGlueLogStreamRegex.FindAllStringSubmatch(b.LogStream, 1)
	if len(streamMatches) != 1 {
		return nil, false
	}
	jobID := streamMatches[0][1]

	out := []RSysLogMessage{}
	for _, event := range b.LogEvents {
		out = append(out, RSysLogMessage{
			Timestamp:   event.Timestamp.Time(),
			ProgramName: env + "--" + app + arnCruft + jobID,
			Hostname:    "aws-glue",
			Message:     event.Message,
		})
	}
	return out, true
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

func stringify(rsyslogs []RSysLogMessage) [][]byte {
	out := make([][]byte, len(rsyslogs))
	for i := range rsyslogs {
		out[i] = []byte(rsyslogs[i].String())
	}
	return out
}

// Split takes a LogEventBatch and separates into a slice of enriched log lines
// Lines are enhanced by adding an Rsyslog prefix, which should be handled correctly by
// the subsequent decoding logic.
func Split(b LogEventBatch) [][]byte {
	if rsyslogMsgs, ok := splitAWSLambda(b); ok {
		return stringify(rsyslogMsgs)
	} else if rsyslogMsgs, ok := splitAWSFargate(b); ok {
		return stringify(rsyslogMsgs)
	} else if rsyslogMsgs, ok := splitAWSBatch(b); ok {
		return stringify(rsyslogMsgs)
	} else if rsyslogMsgs, ok := splitAWSRDS(b); ok {
		return stringify(rsyslogMsgs)
	} else if rsyslogMsgs, ok := splitAWSGlue(b); ok {
		return stringify(rsyslogMsgs)
	}
	return stringify(splitDefault(b))
}
