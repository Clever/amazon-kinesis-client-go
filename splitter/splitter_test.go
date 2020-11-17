package splitter

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"crypto/md5"
	b64 "encoding/base64"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Clever/amazon-kinesis-client-go/decode"
	kpl "github.com/a8m/kinesis-producer"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// In the conversion of CloudWatch LogEvent struct to an RSyslog struct to a string,
	// the timezone used in the final string depends on the locally set timezone.
	// in order for tests to pass, we set TZ to UTC
	os.Setenv("TZ", "UTC")
	os.Exit(m.Run())
}

func TestUnpacking(t *testing.T) {
	input := "H4sIAAAAAAAAADWOTQuCQBRF/8ow6wj6ENRdhLXIClJoERKTvsZHOiPzxiLE/96YtTzcy72n4zUQCQnpuwEe8vXxkJ6O8XUfJclqG/EJ1y8FZkgq3RYvYfMy1pJcUGm5NbptXDZSYg2IekRqb5QbbCxqtcHKgiEeXrJvL3qCsgN2HIuxbtFpWFG7sdky8L1ZECwXc9+b/PUGgXPMfnrspxeydQn5A5VkJYjKlkzfWeGWUInhme1QASEx+qpNeZ/1H1PFPn3yAAAA"

	decoded, err := b64.StdEncoding.DecodeString(input)
	assert.NoError(t, err)

	output, err := unpack(decoded)
	assert.NoError(t, err)

	expectedOutput := LogEventBatch{
		MessageType:         "CONTROL_MESSAGE",
		Owner:               "CloudwatchLogs",
		LogGroup:            "",
		LogStream:           "",
		SubscriptionFilters: []string{},
		LogEvents: []LogEvent{
			{
				ID:        "",
				Timestamp: NewUnixTimestampMillis(1498519943285),
				Message:   "CWL CONTROL MESSAGE: Checking health of destination Kinesis stream.",
			},
		},
	}
	assert.Equal(t, expectedOutput, output)
}

func pack(input LogEventBatch) (string, error) {
	src, err := json.Marshal(input)
	if err != nil {
		return "", err
	}

	// Gzip
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(src); err != nil {
		return "", err
	}
	if err := gz.Flush(); err != nil {
		panic(err)
	}
	if err := gz.Close(); err != nil {
		return "", err
	}

	// Base64 Encode
	return b64.StdEncoding.EncodeToString([]byte(b.String())), nil
}

func TestFullLoop(t *testing.T) {
	input := `{
  "messageType": "DATA_MESSAGE",
  "owner": "123456789012",
  "logGroup": "/aws/batch/job",
  "logStream": "environment--app/default/11111111-2222-3333-4444-555566667777",
  "subscriptionFilters": [
    "MySubscriptionFilter"
  ],
  "logEvents": [
    {
      "id": "33418742379011144044923130086453437181614530551221780480",
      "timestamp": 1498548236012,
      "message": "some log line"
    },
    {
      "id": "33418742387663833181953011865369295871402094815542181889",
      "timestamp": 1498548236400,
      "message": "2017/06/27 07:23:56 Another log line"
    }
  ]
}`

	var leb LogEventBatch
	err := json.Unmarshal([]byte(input), &leb)
	assert.NoError(t, err)

	packed, err := pack(leb)
	assert.NoError(t, err)

	decoded, err := b64.StdEncoding.DecodeString(packed)
	assert.NoError(t, err)

	output, err := unpack(decoded)
	assert.NoError(t, err)

	assert.Equal(t, leb, output)
}

func TestSplitBatch(t *testing.T) {
	input := LogEventBatch{
		MessageType:         "DATA_MESSAGE",
		Owner:               "123456789012",
		LogGroup:            "/aws/batch/job",
		LogStream:           "env--app/default/12345678-1234-1234-1234-555566667777",
		SubscriptionFilters: []string{"MySubscriptionFilter"},
		LogEvents: []LogEvent{
			{
				ID:        "99999992379011144044923130086453437181614530551221780480",
				Timestamp: NewUnixTimestampMillis(1498519943285),
				Message:   "some log line",
			},
			{
				ID:        "99999992387663833181953011865369295871402094815542181889",
				Timestamp: NewUnixTimestampMillis(1498519943285),
				Message:   "another log line",
			},
		},
	}
	lines := Split(input)
	expected := [][]byte{
		[]byte("2017-06-26T23:32:23.285001+00:00 aws-batch env--app/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F12345678-1234-1234-1234-555566667777: some log line"),
		[]byte("2017-06-26T23:32:23.285001+00:00 aws-batch env--app/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F12345678-1234-1234-1234-555566667777: another log line"),
	}
	assert.Equal(t, expected, lines)
}

func TestSplitLambda(t *testing.T) {
	input := LogEventBatch{
		MessageType:         "DATA_MESSAGE",
		Owner:               "123456789012",
		LogGroup:            "/aws/lambda/env--app",
		LogStream:           "2018/01/24/[3]62695bfa96de46938f56b156f5235205",
		SubscriptionFilters: []string{"ForwardLogsToKinesis"},
		LogEvents: []LogEvent{
			{
				ID:        "99999992379011144044923130086453437181614530551221780480",
				Timestamp: NewUnixTimestampMillis(1498519943285),
				Message:   "START RequestId: 8edbd53f-64c7-4a3c-bf1e-efeff40f6512 Version: 3",
			},
			{
				ID:        "99999992387663833181953011865369295871402094815542181889",
				Timestamp: NewUnixTimestampMillis(1498519943285),
				Message:   `{"aws_request_id":"8edbd53f-64c7-4a3c-bf1e-efeff40f6512","level":"info","source":"app","title":"some-log-title"}`,
			},
			{
				ID:        "99999992387663833181953011865369295871402094815542181889",
				Timestamp: NewUnixTimestampMillis(1498519943285),
				Message:   `Example message that doesn't contain a request ID`,
			},
		},
	}
	lines := Split(input)
	expected := [][]byte{
		[]byte(`2017-06-26T23:32:23.285001+00:00 aws-lambda env--app/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F8edbd53f-64c7-4a3c-bf1e-efeff40f6512: START RequestId: 8edbd53f-64c7-4a3c-bf1e-efeff40f6512 Version: 3`),
		[]byte(`2017-06-26T23:32:23.285001+00:00 aws-lambda env--app/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F8edbd53f-64c7-4a3c-bf1e-efeff40f6512: {"aws_request_id":"8edbd53f-64c7-4a3c-bf1e-efeff40f6512","level":"info","source":"app","title":"some-log-title"}`),
		[]byte(`2017-06-26T23:32:23.285001+00:00 aws-lambda env--app/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F12345678-1234-1234-1234-555566667777: Example message that doesn't contain a request ID`),
	}
	assert.Equal(t, expected, lines)
	for i, line := range expected {
		enhanced, err := decode.ParseAndEnhance(string(line), "")
		require.Nil(t, err)
		assert.Equal(t, "aws-lambda", enhanced["hostname"])
		assert.Equal(t, "env", enhanced["container_env"])
		assert.Equal(t, "app", enhanced["container_app"])
		if i != len(expected)-1 /* last line doesn't have a request ID */ {
			assert.Equal(t, "8edbd53f-64c7-4a3c-bf1e-efeff40f6512", enhanced["container_task"])
		}
	}

}

func TestSplitFargate(t *testing.T) {
	input := LogEventBatch{
		MessageType:         "DATA_MESSAGE",
		Owner:               "123456789012",
		LogGroup:            "/ecs/production--clever-com-router",
		LogStream:           "fargate/clever-dev--clever-com-router/27b22d5d68aa4bd3923c95e7f32a3852",
		SubscriptionFilters: []string{"ForwardLogsToKinesis"},
		LogEvents: []LogEvent{
			{
				ID:        "99999992379011144044923130086453437181614530551221780480",
				Timestamp: NewUnixTimestampMillis(1498519943285),
				Message:   "Starting haproxy: haproxy.",
			},
		},
	}
	lines := Split(input)
	expected := [][]byte{
		[]byte(`2017-06-26T23:32:23.285001+00:00 aws-fargate production--clever-com-router/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F27b22d5d68aa4bd3923c95e7f32a3852: Starting haproxy: haproxy.`),
	}
	assert.Equal(t, expected, lines)
	for _, line := range expected {
		enhanced, err := decode.ParseAndEnhance(string(line), "")
		require.Nil(t, err)
		assert.Equal(t, "aws-fargate", enhanced["hostname"])
		assert.Equal(t, "production", enhanced["container_env"])
		assert.Equal(t, "clever-com-router", enhanced["container_app"])
		assert.Equal(t, "27b22d5d68aa4bd3923c95e7f32a3852", enhanced["container_task"])
	}
}

func TestSplitDefault(t *testing.T) {
	input := LogEventBatch{
		MessageType:         "DATA_MESSAGE",
		Owner:               "123456789012",
		LogGroup:            "vpn_flow_logs",
		LogStream:           "eni-43403819-all",
		SubscriptionFilters: []string{"SomeSubscription"},
		LogEvents: []LogEvent{
			{
				ID:        "99999992379011144044923130086453437181614530551221780480",
				Timestamp: NewUnixTimestampMillis(1498519943285),
				Message:   "2 589690932525 eni-43403819 10.0.0.233 172.217.6.46 64067 443 17 8 3969 1516891809 1516891868 ACCEPT OK",
			},
		},
	}
	lines := Split(input)
	expected := [][]byte{
		[]byte(`2017-06-26T23:32:23.285001+00:00 eni-43403819-all vpn_flow_logs--eni-43403819-all/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F12345678-1234-1234-1234-555566667777: 2 589690932525 eni-43403819 10.0.0.233 172.217.6.46 64067 443 17 8 3969 1516891809 1516891868 ACCEPT OK`),
	}
	assert.Equal(t, expected, lines)
}

func TestSplitRDS(t *testing.T) {
	input := LogEventBatch{
		MessageType:         "DATA_MESSAGE",
		Owner:               "123456789012",
		LogGroup:            "/aws/rds/cluster/production-aurora-test-db/slowquery",
		LogStream:           "clever-dev-aurora-test-db",
		SubscriptionFilters: []string{"ForwardLogsToKinesis"},
		LogEvents: []LogEvent{
			{
				ID:        "99999992379011144044923130086453437181614530551221780480",
				Timestamp: NewUnixTimestampMillis(1498519943285),
				Message:   "Slow query: select * from table.",
			},
		},
	}
	lines := Split(input)
	expected := [][]byte{
		[]byte(`2017-06-26T23:32:23.285001+00:00 aws-rds production-aurora-test-db: Slow query: select * from table.`),
	}
	assert.Equal(t, expected, lines)
	for _, line := range expected {
		enhanced, err := decode.ParseAndEnhance(string(line), "")
		require.Nil(t, err)
		assert.Equal(t, "aws-rds", enhanced["hostname"])
		assert.Equal(t, "production-aurora-test-db", enhanced["programname"])
		assert.Equal(t, "Slow query: select * from table.", enhanced["rawlog"])
	}
}

func TestSplitGlue(t *testing.T) {
	input := LogEventBatch{
		MessageType:         "DATA_MESSAGE",
		Owner:               "123456789012",
		LogGroup:            "/aws-glue/jobs/clever-dev/analytics-district-participation/aae75f00",
		LogStream:           "jr_8927660fecacbe026ccab656cb80befea8102ac2023df531b92889b112aada28-1",
		SubscriptionFilters: []string{"ForwardLogsToKinesis"},
		LogEvents: []LogEvent{
			{
				ID:        "99999992379011144044923130086453437181614530551221780480",
				Timestamp: NewUnixTimestampMillis(1498519943285),
				Message:   "foo bar.",
			},
		},
	}
	lines := Split(input)
	expected := [][]byte{
		[]byte(`2017-06-26T23:32:23.285001+00:00 aws-glue clever-dev--analytics-district-participation/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2Fjr_8927660fecacbe026ccab656cb80befea8102ac2023df531b92889b112aada28: foo bar.`),
	}
	assert.Equal(t, expected, lines)
	for _, line := range expected {
		enhanced, err := decode.ParseAndEnhance(string(line), "")
		require.Nil(t, err)
		assert.Equal(t, "aws-glue", enhanced["hostname"])
		assert.Equal(t, "clever-dev", enhanced["container_env"])
		assert.Equal(t, "analytics-district-participation", enhanced["container_app"])
		assert.Equal(t, "jr_8927660fecacbe026ccab656cb80befea8102ac2023df531b92889b112aada28", enhanced["container_task"])
	}
}

func TestSplitIfNecesary(t *testing.T) {

	// We provide three different inputs to batchedWriter.splitMessageIfNecessary
	// plain text
	// zlib compressed text
	// gzip compressed CloudWatch logs batch
	// we verify that the split function matches the input against the correct splitter
	// and decodes it.

	assert := assert.New(t)

	plainTextInput := []byte("hello, world!")

	records, err := SplitMessageIfNecessary(plainTextInput)
	assert.NoError(err)
	assert.Equal(
		records,
		[][]byte{[]byte("hello, world!")},
	)

	var z bytes.Buffer
	zbuf := zlib.NewWriter(&z)
	zbuf.Write([]byte("hello, world!"))
	zbuf.Close()
	zlibSingleInput := z.Bytes()

	records, err = SplitMessageIfNecessary(zlibSingleInput)
	assert.NoError(err)
	assert.Equal(
		records,
		[][]byte{[]byte("hello, world!")},
	)

	// the details of this part aren't super important since the actual functionality is
	// tested in other tests; for this test we just want to make sure that split function
	// correctly realizes it's gzip and call the appropriate CW-log-splitting logic
	var g bytes.Buffer
	gbuf := gzip.NewWriter(&g)
	cwLogBatch := LogEventBatch{
		MessageType:         "test",
		Owner:               "test",
		LogGroup:            "test",
		LogStream:           "test",
		SubscriptionFilters: []string{""},
		LogEvents: []LogEvent{{
			ID:        "test",
			Timestamp: UnixTimestampMillis(time.Date(2020, time.September, 9, 9, 10, 10, 0, time.UTC)),
			Message:   "test",
		}},
	}
	cwLogBatchJSON, _ := json.Marshal(cwLogBatch)
	gbuf.Write(cwLogBatchJSON)
	gbuf.Close()
	gzipBatchInput := g.Bytes()

	expectedRecord := []byte("2020-09-09T09:10:10.000001+00:00 test test--test/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F12345678-1234-1234-1234-555566667777: test")
	records, err = SplitMessageIfNecessary(gzipBatchInput)
	assert.NoError(err)
	assert.Equal(
		records,
		[][]byte{expectedRecord},
	)
}

func createKPLAggregate(input [][]byte, compress bool) []byte {
	var partitionKeyIndex uint64 = 0

	records := []*kpl.Record{}
	for _, log := range input {
		if compress {
			var z bytes.Buffer
			zbuf := zlib.NewWriter(&z)
			zbuf.Write(log)
			zbuf.Close()
			log = z.Bytes()
		}
		records = append(records, &kpl.Record{
			PartitionKeyIndex: &partitionKeyIndex,
			Data:              log,
		})
	}

	logProto, err := proto.Marshal(&kpl.AggregatedRecord{
		PartitionKeyTable: []string{"ecs_task_arn"},
		Records:           records,
	})
	if err != nil {
		panic(err)
	}
	log := append(kplMagicNumber, logProto...)
	logHash := md5.Sum(logProto)
	return append(log, logHash[0:16]...)
}

func TestKPLDeaggregate(t *testing.T) {
	type test struct {
		description string
		input       []byte
		output      [][]byte
		shouldError bool
	}

	tests := []test{
		{
			description: "non-aggregated record",
			input:       []byte("hello"),
			output:      [][]byte{[]byte("hello")},
			shouldError: false,
		},
		{
			description: "one kpl-aggregated record",
			input: createKPLAggregate(
				[][]byte{[]byte("hello")},
				false,
			),
			output:      [][]byte{[]byte("hello")},
			shouldError: false,
		},
		{
			description: "three kpl-aggregated record",
			input: createKPLAggregate([][]byte{
				[]byte("hello, "),
				[]byte("world"),
				[]byte("!"),
			},
				false,
			),
			output: [][]byte{
				[]byte("hello, "),
				[]byte("world"),
				[]byte("!"),
			},
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			out, err := KPLDeaggregate(tt.input)
			if tt.shouldError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, out, tt.output)
		})

	}
}

func TestDeaggregate(t *testing.T) {
	type test struct {
		description string
		input       []byte
		output      [][]byte
		shouldError bool
	}

	tests := []test{
		{
			description: "non-aggregated record",
			input:       []byte("hello"),
			output:      [][]byte{[]byte("hello")},
			shouldError: false,
		},
		{
			description: "one kpl-aggregated record",
			input: createKPLAggregate(
				[][]byte{[]byte("hello")},
				false,
			),
			output:      [][]byte{[]byte("hello")},
			shouldError: false,
		},
		{
			description: "three kpl-aggregated record",
			input: createKPLAggregate([][]byte{
				[]byte("hello, "),
				[]byte("world"),
				[]byte("!"),
			},
				false,
			),
			output: [][]byte{
				[]byte("hello, "),
				[]byte("world"),
				[]byte("!"),
			},
			shouldError: false,
		},
		{
			description: "one kpl-aggregated zlib record",
			input: createKPLAggregate(
				[][]byte{[]byte("hello")},
				true,
			),
			output:      [][]byte{[]byte("hello")},
			shouldError: false,
		},
		{
			description: "three kpl-aggregated zlib record",
			input: createKPLAggregate([][]byte{
				[]byte("hello, "),
				[]byte("world"),
				[]byte("!"),
			},
				true,
			),
			output: [][]byte{
				[]byte("hello, "),
				[]byte("world"),
				[]byte("!"),
			},
			shouldError: false,
		},
	}

	var g bytes.Buffer
	gbuf := gzip.NewWriter(&g)
	cwLogBatch := LogEventBatch{
		MessageType:         "test",
		Owner:               "test",
		LogGroup:            "test",
		LogStream:           "test",
		SubscriptionFilters: []string{""},
		LogEvents: []LogEvent{{
			ID:        "test",
			Timestamp: UnixTimestampMillis(time.Date(2020, time.September, 9, 9, 10, 10, 0, time.UTC)),
			Message:   "test",
		}},
	}
	cwLogBatchJSON, _ := json.Marshal(cwLogBatch)
	gbuf.Write(cwLogBatchJSON)
	gbuf.Close()
	gzipBatchInput := g.Bytes()

	tests = append(tests, test{
		description: "cloudwatch log batch",
		input:       gzipBatchInput,
		output:      [][]byte{[]byte("2020-09-09T09:10:10.000001+00:00 test test--test/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F12345678-1234-1234-1234-555566667777: test")},
		shouldError: false,
	})
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			out, err := Deaggregate(tt.input)

			if tt.shouldError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, out, tt.output)
		})

	}
}
