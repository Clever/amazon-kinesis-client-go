package decode

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Clever/syslogparser"
	"github.com/stretchr/testify/assert"
)

const RFC3339Micro = "2006-01-02T15:04:05.999999-07:00"

type Spec struct {
	Title          string
	Input          string
	ExpectedOutput map[string]interface{}
	ExpectedError  error
}

func TestKayveeDecoding(t *testing.T) {
	specs := []Spec{
		Spec{
			Title: "handles just JSON",
			Input: `{"a":"b"}`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "",
				"postfix": "",
				"a":       "b",
				"type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "handles prefix + JSON",
			Input: `prefix {"a":"b"}`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "prefix ",
				"postfix": "",
				"a":       "b",
				"type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "handles JSON + postfix",
			Input: `{"a":"b"} postfix`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "",
				"postfix": " postfix",
				"a":       "b",
				"type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "handles prefix + JSON + postfix",
			Input: `prefix {"a":"b"} postfix`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "prefix ",
				"postfix": " postfix",
				"a":       "b",
				"type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title:          "Returns NonKayveeError if not JSON in body",
			Input:          `prefix { postfix`,
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &NonKayveeError{},
		},
		Spec{
			Title:          "errors on invalid JSON (missing a quote)",
			Input:          `prefix {"a:"b"} postfix`,
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &json.SyntaxError{},
		},
	}

	for _, spec := range specs {
		t.Run(fmt.Sprintf(spec.Title), func(t *testing.T) {
			assert := assert.New(t)
			fields, err := FieldsFromKayvee(spec.Input)
			if spec.ExpectedError != nil {
				assert.Error(err)
				assert.IsType(spec.ExpectedError, err)
			} else {
				assert.NoError(err)
			}
			assert.Equal(spec.ExpectedOutput, fields)
		})
	}
}

func TestSyslogDecoding(t *testing.T) {
	// timestamps in Rsyslog_TraditionalFileFormat
	logTime, err := time.Parse(time.Stamp, "Oct 25 10:20:37")
	if err != nil {
		t.Fatal(err)
	}
	// parsing assumes log is from the current year
	logTime = logTime.AddDate(time.Now().Year(), 0, 0).UTC()

	logTime2, err := time.Parse(time.Stamp, "Apr  5 21:45:54")
	if err != nil {
		t.Fatal(err)
	}
	logTime2 = logTime2.AddDate(time.Now().Year(), 0, 0).UTC()

	// timestamp in Rsyslog_FileFormat
	logTime3, err := time.Parse(RFC3339Micro, "2017-04-05T21:57:46.794862+00:00")
	if err != nil {
		t.Fatal(err)
	}
	logTime3 = logTime3.UTC()

	specs := []Spec{
		Spec{
			Title: "Parses Rsyslog_TraditionalFileFormat with simple log body",
			Input: `Oct 25 10:20:37 some-host docker/fa3a5e338a47[1294]: log body`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":   logTime,
				"hostname":    "some-host",
				"programname": "docker/fa3a5e338a47",
				"rawlog":      "log body",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Parses Rsyslog_TraditionalFileFormat with haproxy access log body",
			Input: `Apr  5 21:45:54 influx-service docker/0000aa112233[1234]: [httpd] 2017/04/05 21:45:54 172.17.42.1 - heka [05/Apr/2017:21:45:54 +0000] POST /write?db=foo&precision=ms HTTP/1.1 204 0 - Go 1.1 package http 123456-1234-1234-b11b-000000000000 13.688672ms`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":   logTime2,
				"hostname":    "influx-service",
				"programname": "docker/0000aa112233",
				"rawlog":      "[httpd] 2017/04/05 21:45:54 172.17.42.1 - heka [05/Apr/2017:21:45:54 +0000] POST /write?db=foo&precision=ms HTTP/1.1 204 0 - Go 1.1 package http 123456-1234-1234-b11b-000000000000 13.688672ms",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Parses Rsyslog_TraditionalFileFormat",
			Input: `Apr  5 21:45:54 mongodb-some-machine whackanop: 2017/04/05 21:46:11 found 0 ops`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":   logTime2,
				"hostname":    "mongodb-some-machine",
				"programname": "whackanop",
				"rawlog":      "2017/04/05 21:46:11 found 0 ops",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Parses Rsyslog_ FileFormat with Kayvee payload",
			Input: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":   logTime3,
				"hostname":    "ip-10-0-0-0",
				"programname": `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":      `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
			},
			ExpectedError: nil,
		},
		Spec{
			Title:          "Fails to parse non-RSyslog log line",
			Input:          `not rsyslog`,
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &syslogparser.ParserError{},
		},
	}
	for _, spec := range specs {
		t.Run(fmt.Sprintf(spec.Title), func(t *testing.T) {
			assert := assert.New(t)
			fields, err := FieldsFromSyslog(spec.Input)
			if spec.ExpectedError != nil {
				assert.Error(err)
				assert.IsType(spec.ExpectedError, err)
			} else {
				assert.NoError(err)
			}
			assert.Equal(spec.ExpectedOutput, fields)
		})
	}
}

type ParseAndEnhanceInput struct {
	Line                   string
	StringifyNested        bool
	RenameESReservedFields bool
	MinimumTimestamp       time.Time
}

type ParseAndEnhanceSpec struct {
	Title          string
	Input          ParseAndEnhanceInput
	ExpectedOutput map[string]interface{}
	ExpectedError  error
}

func TestParseAndEnhance(t *testing.T) {
	// timestamp in Rsyslog_FileFormat
	logTime3, err := time.Parse(RFC3339Micro, "2017-04-05T21:57:46.794862+00:00")
	if err != nil {
		t.Fatal(err)
	}
	logTime3 = logTime3.UTC()

	specs := []ParseAndEnhanceSpec{
		ParseAndEnhanceSpec{
			Title: "Parses a Kayvee log line from an ECS app",
			Input: ParseAndEnhanceInput{Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Parses a Kayvee log line from an ECS app, with override to container_app",
			Input: ParseAndEnhanceInput{Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished","container_app":"force-app"}`},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished","container_app":"force-app"}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "force-app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Parses a non-Kayvee log line",
			Input: ParseAndEnhanceInput{Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: some log`},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `some log`,
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title:          "Fails to parse non-RSyslog log line",
			Input:          ParseAndEnhanceInput{Line: `not rsyslog`},
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &syslogparser.ParserError{},
		},
		ParseAndEnhanceSpec{
			Title: "Parses JSON values",
			Input: ParseAndEnhanceInput{Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": {"a":"b"}}`},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": {"a":"b"}}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"nested":         map[string]interface{}{"a": "b"},
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Has the option to stringify object values",
			Input: ParseAndEnhanceInput{
				Line:            `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": {"a":"b"}}`,
				StringifyNested: true,
			},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": {"a":"b"}}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"nested":         `{"a":"b"}`,
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Has the option to stringify array values",
			Input: ParseAndEnhanceInput{
				Line:            `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": [{"a":"b"}]}`,
				StringifyNested: true,
			},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": [{"a":"b"}]}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"nested":         `[{"a":"b"}]`,
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Has the option to rename reserved ES fields",
			Input: ParseAndEnhanceInput{
				Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				RenameESReservedFields: true,
			},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"kv__source":     "a",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Errors if logTime < MinimumTimestamp",
			Input: ParseAndEnhanceInput{
				Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				RenameESReservedFields: true,
				MinimumTimestamp:       time.Now().Add(100 * time.Hour * 24 * 365), // good thru year 2117
			},
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  fmt.Errorf(""),
		},
		ParseAndEnhanceSpec{
			Title: "Accepts logs if logTime > MinimumTimestamp",
			Input: ParseAndEnhanceInput{
				Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				RenameESReservedFields: true,
				MinimumTimestamp:       time.Now().Add(-100 * time.Hour * 24 * 365), // good thru year 2117
			},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"kv__source":     "a",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Accepts logs if logTime > MinimumTimestamp",
			Input: ParseAndEnhanceInput{
				Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				RenameESReservedFields: true,
				MinimumTimestamp:       time.Now().Add(-100 * time.Hour * 24 * 365), // good thru year 2117
			},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"kv__source":     "a",
			},
			ExpectedError: nil,
		},
	}
	for _, spec := range specs {
		t.Run(fmt.Sprintf(spec.Title), func(t *testing.T) {
			assert := assert.New(t)
			fields, err := ParseAndEnhance(spec.Input.Line, "deploy-env", spec.Input.StringifyNested, spec.Input.RenameESReservedFields, spec.Input.MinimumTimestamp)
			if spec.ExpectedError != nil {
				assert.Error(err)
				assert.IsType(spec.ExpectedError, err)
			} else {
				assert.NoError(err)
			}
			assert.Equal(spec.ExpectedOutput, fields)
		})
	}
}

func TestGetContainerMeta(t *testing.T) {
	assert := assert.New(t)

	t.Log("Must have a programname to get container meta")
	programname := ""
	_, err := getContainerMeta(programname, "", "", "")
	assert.Error(err)

	t.Log("Can parse a programname")
	programname = `env1--app2/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`
	meta, err := getContainerMeta(programname, "", "", "")
	assert.NoError(err)
	assert.Equal(map[string]string{
		"container_env":  "env1",
		"container_app":  "app2",
		"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
	}, meta)

	t.Log("Can override just 'env'")
	overrideEnv := "force-env"
	meta, err = getContainerMeta(programname, overrideEnv, "", "")
	assert.NoError(err)
	assert.Equal(map[string]string{
		"container_env":  overrideEnv,
		"container_app":  "app2",
		"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
	}, meta)

	t.Log("Can override just 'app'")
	overrideApp := "force-app"
	meta, err = getContainerMeta(programname, "", overrideApp, "")
	assert.NoError(err)
	assert.Equal(map[string]string{
		"container_env":  "env1",
		"container_app":  overrideApp,
		"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
	}, meta)

	t.Log("Can override just 'task'")
	overrideTask := "force-task"
	meta, err = getContainerMeta(programname, "", "", overrideTask)
	assert.NoError(err)
	assert.Equal(map[string]string{
		"container_env":  "env1",
		"container_app":  "app2",
		"container_task": overrideTask,
	}, meta)

	t.Log("Can override all fields")
	programname = `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`
	meta, err = getContainerMeta(programname, overrideEnv, overrideApp, overrideTask)
	assert.NoError(err)
	assert.Equal(map[string]string{
		"container_env":  overrideEnv,
		"container_app":  overrideApp,
		"container_task": overrideTask,
	}, meta)
}

func TestExtractKVMeta(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		Description                string
		Log                        map[string]interface{}
		Team                       string
		Language                   string
		Version                    string
		ExpectedMetricsRoutes      []MetricsRoute
		ExpectedAnalyticsRoutes    []AnalyticsRoute
		ExpectedNotificationRoutes []NotificationRoute
		ExpectedAlertRoutes        []AlertRoute
	}{
		{
			Description: "log line with no routes",
			Log:         map[string]interface{}{"hi": "hello!"},
		},
		{
			Description: "empty _kvmeta",
			Log: map[string]interface{}{
				"hi":      "hello!",
				"_kvmeta": map[string]interface{}{},
			},
		},
		{
			Description: "_kvmeta with no routes",
			Team:        "green",
			Version:     "three",
			Language:    "tree",
			Log: map[string]interface{}{
				"hi": "hello!",
				"_kvmeta": map[string]interface{}{
					"team":        "green",
					"kv_version":  "three",
					"kv_language": "tree",
				},
			},
		},
		{
			Description: "_kvmeta with metric routes",
			Team:        "green",
			Version:     "three",
			Language:    "tree",
			ExpectedMetricsRoutes: []MetricsRoute{
				{
					Series:     "1,1,2,3,5,8,13",
					Dimensions: []string{"app", "district"},
					ValueField: "value",
					RuleName:   "cool",
				},
				{
					Series:     "1,1,2,6,24,120,720,5040",
					Dimensions: []string{"app", "district"},
					ValueField: "value",
					RuleName:   "cool2",
				},
			},
			Log: map[string]interface{}{
				"hi": "hello!",
				"_kvmeta": map[string]interface{}{
					"team":        "green",
					"kv_version":  "three",
					"kv_language": "tree",
					"routes": []map[string]interface{}{
						map[string]interface{}{
							"type":        "metrics",
							"rule":        "cool",
							"series":      "1,1,2,3,5,8,13",
							"value_field": "value",
							"dimensions":  []string{"app", "district"},
						},
						map[string]interface{}{
							"type":       "metrics",
							"rule":       "cool2",
							"series":     "1,1,2,6,24,120,720,5040",
							"dimensions": []string{"app", "district"},
						},
					},
				},
			},
		},
		{
			Description: "_kvmeta with analytic routes",
			Team:        "green",
			Version:     "christmas",
			Language:    "tree",
			ExpectedAnalyticsRoutes: []AnalyticsRoute{
				{
					Series:   "what-is-this",
					RuleName: "what's-this?",
				},
				{
					RuleName: "there's-app-invites-everywhere",
					Series:   "there's-bts-in-the-air",
				},
			},
			Log: map[string]interface{}{
				"hi": "hello!",
				"_kvmeta": map[string]interface{}{
					"team":        "green",
					"kv_version":  "christmas",
					"kv_language": "tree",
					"routes": []map[string]interface{}{
						map[string]interface{}{
							"type":   "analytics",
							"rule":   "what's-this?",
							"series": "what-is-this",
						},
						map[string]interface{}{
							"type":   "analytics",
							"rule":   "there's-app-invites-everywhere",
							"series": "there's-bts-in-the-air",
						},
					},
				},
			},
		},
		{
			Description: "_kvmeta with notification routes",
			Team:        "slack",
			Version:     "evergreen",
			Language:    "markdown-ish",
			ExpectedNotificationRoutes: []NotificationRoute{
				{
					RuleName: "did-you-know",
					Channel:  "originally-slack",
					Message:  "was a gaming company",
					Icon:     ":video_game:",
					User:     "og slack bronie",
				},
				{
					RuleName: "what's-the-catch",
					Channel:  "slack-is-built-with-php",
					Message:  "just like farmville",
					Icon:     ":ghost:",
					User:     "logging-pipeline",
				},
			},
			Log: map[string]interface{}{
				"hi": "hello!",
				"_kvmeta": map[string]interface{}{
					"team":        "slack",
					"kv_version":  "evergreen",
					"kv_language": "markdown-ish",
					"routes": []map[string]interface{}{
						map[string]interface{}{
							"type":    "notifications",
							"rule":    "did-you-know",
							"channel": "originally-slack",
							"message": "was a gaming company",
							"icon":    ":video_game:",
							"user":    "og slack bronie",
						},
						map[string]interface{}{
							"type":    "notifications",
							"rule":    "what's-the-catch",
							"channel": "slack-is-built-with-php",
							"message": "just like farmville",
						},
					},
				},
			},
		},
		{
			Description: "_kvmeta with alert routes",
			Team:        "a-team",
			Version:     "old",
			Language:    "jive",
			ExpectedAlertRoutes: []AlertRoute{
				{
					RuleName:   "last-call",
					Series:     "doing-it-til-we-fall",
					Dimensions: []string{"who", "where"},
					StatType:   "guage",
					ValueField: "status",
				},
				{
					RuleName:   "watch-out-now",
					Series:     "dem-gators-gonna-bite-ya",
					Dimensions: []string{"how-fresh", "how-clean"},
					StatType:   "counter",
					ValueField: "value",
				},
			},
			Log: map[string]interface{}{
				"hi": "hello!",
				"_kvmeta": map[string]interface{}{
					"team":        "a-team",
					"kv_version":  "old",
					"kv_language": "jive",
					"routes": []map[string]interface{}{
						map[string]interface{}{
							"type":        "alerts",
							"rule":        "last-call",
							"series":      "doing-it-til-we-fall",
							"dimensions":  []string{"who", "where"},
							"stat_type":   "guage",
							"value_field": "status",
						},
						map[string]interface{}{
							"type":       "alerts",
							"rule":       "watch-out-now",
							"series":     "dem-gators-gonna-bite-ya",
							"dimensions": []string{"how-fresh", "how-clean"},
						},
					},
				},
			},
		},
		{
			Description: "_kvmeta with all types of routes",
			Team:        "diversity",
			Version:     "kv-routes",
			Language:    "understanding",
			ExpectedMetricsRoutes: []MetricsRoute{
				{
					RuleName:   "all-combos",
					Series:     "1,1,2,6,24,120,720,5040",
					Dimensions: []string{"fact", "orial"},
					ValueField: "value",
				},
			},
			ExpectedAnalyticsRoutes: []AnalyticsRoute{
				{
					RuleName: "there's-app-invites-everywhere",
					Series:   "there's-bts-in-the-air",
				},
			},
			ExpectedNotificationRoutes: []NotificationRoute{
				{
					RuleName: "what's-the-catch",
					Channel:  "slack-is-built-with-php",
					Message:  "just like farmville",
					Icon:     ":ghost:",
					User:     "logging-pipeline",
				},
			},
			ExpectedAlertRoutes: []AlertRoute{
				{
					RuleName:   "last-call",
					Series:     "doing-it-til-we-fall",
					Dimensions: []string{"who", "where"},
					StatType:   "guage",
					ValueField: "status",
				},
			},
			Log: map[string]interface{}{
				"hi": "hello!",
				"_kvmeta": map[string]interface{}{
					"team":        "diversity",
					"kv_version":  "kv-routes",
					"kv_language": "understanding",
					"routes": []map[string]interface{}{
						map[string]interface{}{
							"type":       "metrics",
							"rule":       "all-combos",
							"series":     "1,1,2,6,24,120,720,5040",
							"dimensions": []string{"fact", "orial"},
						},
						map[string]interface{}{
							"type":   "analytics",
							"rule":   "there's-app-invites-everywhere",
							"series": "there's-bts-in-the-air",
						},
						map[string]interface{}{
							"type":    "notifications",
							"rule":    "what's-the-catch",
							"channel": "slack-is-built-with-php",
							"message": "just like farmville",
						},
						map[string]interface{}{
							"type":        "alerts",
							"rule":        "last-call",
							"series":      "doing-it-til-we-fall",
							"dimensions":  []string{"who", "where"},
							"stat_type":   "guage",
							"value_field": "status",
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Log(test.Description)
		kvmeta := ExtractKVMeta(test.Log)

		assert.Equal(test.Team, kvmeta.Team)
		assert.Equal(test.Language, kvmeta.Language)
		assert.Equal(test.Version, kvmeta.Version)

		assert.Equal(len(test.ExpectedMetricsRoutes), len(kvmeta.Routes.MetricsRoutes()))
		for idx, route := range kvmeta.Routes.MetricsRoutes() {
			expected := test.ExpectedMetricsRoutes[idx]
			assert.Exactly(expected, route)
		}
		assert.Equal(len(test.ExpectedAnalyticsRoutes), len(kvmeta.Routes.AnalyticsRoutes()))
		for idx, route := range kvmeta.Routes.AnalyticsRoutes() {
			expected := test.ExpectedAnalyticsRoutes[idx]
			assert.Exactly(expected, route)
		}
		assert.Equal(len(test.ExpectedNotificationRoutes), len(kvmeta.Routes.NotificationRoutes()))
		for idx, route := range kvmeta.Routes.NotificationRoutes() {
			expected := test.ExpectedNotificationRoutes[idx]
			assert.Exactly(expected, route)
		}
		assert.Equal(len(test.ExpectedAlertRoutes), len(kvmeta.Routes.AlertRoutes()))
		for idx, route := range kvmeta.Routes.AlertRoutes() {
			expected := test.ExpectedAlertRoutes[idx]
			assert.Exactly(expected, route)
		}
	}
}

// Benchmarks
const benchmarkLine = `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`

func BenchmarkFieldsFromKayvee(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := FieldsFromKayvee(benchmarkLine)
		if err != nil {
			b.FailNow()
		}
	}
}

func BenchmarkFieldsFromSyslog(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := FieldsFromSyslog(benchmarkLine)
		if err != nil {
			b.FailNow()
		}
	}
}

func BenchmarkParseAndEnhance(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := ParseAndEnhance(benchmarkLine, "env", false, false, time.Time{})
		if err != nil {
			b.FailNow()
		}
	}
}
