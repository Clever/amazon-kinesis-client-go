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

	output, err := unpack(string(decoded))
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
				Timestamp: 1498519943285,
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
  "logStream": "environment--app/11111111-2222-3333-4444-555566667777/88889999-0000-aaa-bbbb-ccccddddeeee",
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

	output, err := unpack(string(decoded))
	assert.NoError(t, err)

	assert.Equal(t, leb, output)
}

func TestSplit(t *testing.T) {
	input := LogEventBatch{
		MessageType:         "DATA_MESSAGE",
		Owner:               "123456789012",
		LogGroup:            "/aws/batch/job",
		LogStream:           "env--app/12345678-1234-1234-1234-555566667777/88889999-0000-aaaa-bbbb-ccccddddeeee",
		SubscriptionFilters: []string{"MySubscriptionFilter"},
		LogEvents: []LogEvent{
			{
				ID:        "99999992379011144044923130086453437181614530551221780480",
				Timestamp: 1498519943285,
				Message:   "some log line",
			},
			{
				ID:        "99999992387663833181953011865369295871402094815542181889",
				Timestamp: 1498519943285,
				Message:   "another log line",
			},
		},
	}
	prodEnv := false
	lines := Split(input, prodEnv)
	expected := [][]byte{
		"2017-06-26T23:32:23.285001+00:00 aws-batch env--app/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F12345678-1234-1234-1234-555566667777[1]: some log line",
		"2017-06-26T23:32:23.285001+00:00 aws-batch env--app/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F12345678-1234-1234-1234-555566667777[1]: another log line",
	}
	assert.Equal(t, expected, lines)
}

func TestSplitFiltersByEnv(t *testing.T) {
	t.Log("If Split is run with prodEnv == true, it should omit logs with env != production")
	input := LogEventBatch{
		MessageType: "DATA_MESSAGE",
		Owner:       "123456789012",
		LogGroup:    "/aws/batch/job",
		LogStream:   "env--app/12345678-1234-1234-1234-555566667777/88889999-0000-aaaa-bbbb-ccccddddeeee",
		// LogStream:           "environment--app",
		SubscriptionFilters: []string{"MySubscriptionFilter"},
		LogEvents: []LogEvent{
			{
				ID:        "99999992379011144044923130086453437181614530551221780480",
				Timestamp: 1498519943285,
				Message:   "some log line",
			},
			{
				ID:        "99999992387663833181953011865369295871402094815542181889",
				Timestamp: 1498519943285,
				Message:   "another log line",
			},
		},
	}
	prodEnv := true
	lines := Split(input, prodEnv)
	expected := [][]byte{}
	assert.Equal(t, expected, lines)

	t.Log("If Split is run with prodEnv == false, it should omit logs with env == production")
	input = LogEventBatch{
		MessageType: "DATA_MESSAGE",
		Owner:       "123456789012",
		LogGroup:    "/aws/batch/job",
		LogStream:   "production--app/12345678-1234-1234-1234-555566667777/88889999-0000-aaaa-bbbb-ccccddddeeee",
		// LogStream:           "environment--app",
		SubscriptionFilters: []string{"MySubscriptionFilter"},
		LogEvents: []LogEvent{
			{
				ID:        "99999992379011144044923130086453437181614530551221780480",
				Timestamp: 1498519943285,
				Message:   "some log line",
			},
			{
				ID:        "99999992387663833181953011865369295871402094815542181889",
				Timestamp: 1498519943285,
				Message:   "another log line",
			},
		},
	}
	prodEnv = false
	lines = Split(input, prodEnv)
	expected = [][]byte{}
	assert.Equal(t, expected, lines)
}
