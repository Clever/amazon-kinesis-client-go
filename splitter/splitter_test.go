package splitter

import (
	"bytes"
	"compress/gzip"
	b64 "encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		[]byte("2017-06-26T23:32:23.285001+00:00 aws-batch env--app/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F12345678-1234-1234-1234-555566667777[1]: some log line"),
		[]byte("2017-06-26T23:32:23.285001+00:00 aws-batch env--app/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F12345678-1234-1234-1234-555566667777[1]: another log line"),
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
		},
	}
	lines := Split(input)
	expected := [][]byte{
		[]byte(`2017-06-26T23:32:23.285001+00:00 aws-lambda env--app/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F8edbd53f-64c7-4a3c-bf1e-efeff40f6512[1]: START RequestId: 8edbd53f-64c7-4a3c-bf1e-efeff40f6512 Version: 3`),
		[]byte(`2017-06-26T23:32:23.285001+00:00 aws-lambda env--app/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F8edbd53f-64c7-4a3c-bf1e-efeff40f6512[1]: {"aws_request_id":"8edbd53f-64c7-4a3c-bf1e-efeff40f6512","level":"info","source":"app","title":"some-log-title"}`),
	}
	assert.Equal(t, expected, lines)
}
