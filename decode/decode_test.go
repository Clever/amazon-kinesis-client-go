package decode

import (
	"errors"
	"fmt"
	"sort"
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
				"prefix":           "",
				"postfix":          "",
				"a":                "b",
				"decoder_msg_type": "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "handles prefix + JSON",
			Input: `prefix {"a":"b"}`,
			ExpectedOutput: map[string]interface{}{
				"prefix":           "prefix ",
				"postfix":          "",
				"a":                "b",
				"decoder_msg_type": "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "handles JSON + postfix",
			Input: `{"a":"b"} postfix`,
			ExpectedOutput: map[string]interface{}{
				"prefix":           "",
				"postfix":          " postfix",
				"a":                "b",
				"decoder_msg_type": "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "handles prefix + JSON + postfix",
			Input: `prefix {"a":"b"} postfix`,
			ExpectedOutput: map[string]interface{}{
				"prefix":           "prefix ",
				"postfix":          " postfix",
				"a":                "b",
				"decoder_msg_type": "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Reserved fields are respected",
			Input: `prefix {"a":"b","prefix":"no-override","postfix":"no-override",` +
				`"decoder_msg_type":"no-override","timestamp":"no-override",` +
				`"hostname":"no-override","rawlog":"no-override"} postfix`,
			ExpectedOutput: map[string]interface{}{
				"prefix":           "prefix ",
				"postfix":          " postfix",
				"a":                "b",
				"decoder_msg_type": "Kayvee",
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
			ExpectedError:  &NonKayveeError{},
		},
		Spec{
			Title:          "errors on empty JSON: {}",
			Input:          `prefix {} postfix`,
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &NonKayveeError{},
		},
	}

	for _, spec := range specs {
		t.Run(fmt.Sprint(spec.Title), func(t *testing.T) {
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
				"timestamp":        logTime,
				"hostname":         "some-host",
				"programname":      "docker/fa3a5e338a47",
				"rawlog":           "log body",
				"decoder_msg_type": "syslog",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Parses Rsyslog_TraditionalFileFormat with haproxy access log body",
			Input: `Apr  5 21:45:54 influx-service docker/0000aa112233[1234]: [httpd] 2017/04/05 21:45:54 172.17.42.1 - heka [05/Apr/2017:21:45:54 +0000] POST /write?db=foo&precision=ms HTTP/1.1 204 0 - Go 1.1 package http 123456-1234-1234-b11b-000000000000 13.688672ms`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime2,
				"hostname":         "influx-service",
				"programname":      "docker/0000aa112233",
				"rawlog":           "[httpd] 2017/04/05 21:45:54 172.17.42.1 - heka [05/Apr/2017:21:45:54 +0000] POST /write?db=foo&precision=ms HTTP/1.1 204 0 - Go 1.1 package http 123456-1234-1234-b11b-000000000000 13.688672ms",
				"decoder_msg_type": "syslog",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Parses Rsyslog_TraditionalFileFormat",
			Input: `Apr  5 21:45:54 mongodb-some-machine whackanop: 2017/04/05 21:46:11 found 0 ops`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime2,
				"hostname":         "mongodb-some-machine",
				"programname":      "whackanop",
				"rawlog":           "2017/04/05 21:46:11 found 0 ops",
				"decoder_msg_type": "syslog",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Parses Rsyslog_ FileFormat with Kayvee payload",
			Input: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime3,
				"hostname":         "ip-10-0-0-0",
				"programname":      `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":           `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
				"decoder_msg_type": "syslog",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Parses Rsyslog_ FileFormat without PID",
			Input: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime3,
				"hostname":         "ip-10-0-0-0",
				"programname":      `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":           `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
				"decoder_msg_type": "syslog",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Parses Rsyslog_ FileFormat with simple log body for slowquery",
			Input: `2017-04-05T21:57:46.794862+00:00 aws-rds production-aurora-test-db: Slow query: select * from table.`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime3,
				"hostname":         "aws-rds",
				"programname":      "production-aurora-test-db",
				"rawlog":           "Slow query: select * from table.",
				"decoder_msg_type": "syslog",
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
		t.Run(fmt.Sprint(spec.Title), func(t *testing.T) {
			assert := assert.New(t)
			fields, err := FieldsFromSyslog(spec.Input)
			if spec.ExpectedError != nil {
				assert.Error(err)
				assert.True(errors.As(err, &spec.ExpectedError))
				return
			}
			assert.NoError(err)
			assert.Equal(spec.ExpectedOutput, fields)
		})
	}
}

type ParseAndEnhanceSpec struct {
	Title          string
	Line           string
	ExpectedOutput map[string]interface{}
	ExpectedError  error
}

func TestParseAndEnhance(t *testing.T) {
	logTime2, err := time.Parse(time.RFC3339, "2017-04-05T21:57:46+00:00")
	if err != nil {
		t.Fatal(err)
	}
	logTime2 = logTime2.UTC()

	// timestamp in Rsyslog_FileFormat
	logTime3, err := time.Parse(RFC3339Micro, "2017-04-05T21:57:46.794862+00:00")
	if err != nil {
		t.Fatal(err)
	}
	logTime3 = logTime3.UTC()

	logTime4, err := time.Parse(fluentTimeFormat, "2020-08-13T21:10:57.000+0000")
	if err != nil {
		t.Fatal(err)
	}

	specs := []ParseAndEnhanceSpec{
		ParseAndEnhanceSpec{
			Title: "Parses a Kayvee log line from an ECS app",
			Line:  `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime3,
				"hostname":         "ip-10-0-0-0",
				"programname":      `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":           `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
				"title":            "request_finished",
				"decoder_msg_type": "Kayvee",
				"prefix":           "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":          "",
				"env":              "deploy-env",
				"container_env":    "env",
				"container_app":    "app",
				"container_task":   "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Parses a Kayvee log line from an ECS app, with override to container_app",
			Line:  `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished","container_app":"force-app"}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime3,
				"hostname":         "ip-10-0-0-0",
				"programname":      `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":           `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished","container_app":"force-app"}`,
				"title":            "request_finished",
				"decoder_msg_type": "Kayvee",
				"prefix":           "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":          "",
				"env":              "deploy-env",
				"container_env":    "env",
				"container_app":    "force-app",
				"container_task":   "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Parses a non-Kayvee log line",
			Line:  `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: some log`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime3,
				"hostname":         "ip-10-0-0-0",
				"programname":      `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":           `some log`,
				"env":              "deploy-env",
				"decoder_msg_type": "syslog",
				"container_env":    "env",
				"container_app":    "app",
				"container_task":   "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title:          "Fails to parse non-RSyslog, non-Fluent log line",
			Line:           `not rsyslog`,
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  fmt.Errorf(""),
		},
		ParseAndEnhanceSpec{
			Title: "Parses JSON values",
			Line:  `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": {"a":"b"}}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime3,
				"hostname":         "ip-10-0-0-0",
				"programname":      `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":           `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": {"a":"b"}}`,
				"title":            "request_finished",
				"decoder_msg_type": "Kayvee",
				"prefix":           "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":          "",
				"env":              "deploy-env",
				"container_env":    "env",
				"container_app":    "app",
				"container_task":   "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"nested":           map[string]interface{}{"a": "b"},
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Log with timestamp time.RFC3339 format",
			Line:  `2017-04-05T21:57:46+00:00 mongo-docker-pipeline-r10-4 diamond[24099] Signal Received: 15`,
			ExpectedOutput: map[string]interface{}{
				"env":              "deploy-env",
				"hostname":         "mongo-docker-pipeline-r10-4",
				"programname":      "diamond",
				"decoder_msg_type": "syslog",
				"rawlog":           "Signal Received: 15",
				"timestamp":        logTime2,
			},
		},
		ParseAndEnhanceSpec{
			Title: "RDS Slowquery Log rdsadmin",
			Line: `2017-04-05T21:57:46+00:00 aws-rds production-aurora-test-db: Slow query: # Time: 190921 16:02:59
# User@Host: rdsadmin[rdsadmin] @ localhost []  Id:     1
# Query_time: 22.741550  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 0SET timestamp=1569081779;call action start_seamless_scaling('AQEAAB1P/PAIqvTHEQFJAEkojZUoH176FGJttZ62JF5QmRehaf0S0VFTa+5MPJdYQ9k0/sekBlnMi8U=', 300000, 2);
SET timestamp=1569862702;`,
			ExpectedOutput: map[string]interface{}{
				"env":              "deploy-env",
				"hostname":         "aws-rds",
				"programname":      "production-aurora-test-db",
				"decoder_msg_type": "syslog",
				"rawlog":           "Slow query: # Time: 190921 16:02:59\n# User@Host: rdsadmin[rdsadmin] @ localhost []  Id:     1\n# Query_time: 22.741550  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 0SET timestamp=1569081779;call action start_seamless_scaling('AQEAAB1P/PAIqvTHEQFJAEkojZUoH176FGJttZ62JF5QmRehaf0S0VFTa+5MPJdYQ9k0/sekBlnMi8U=', 300000, 2);\nSET timestamp=1569862702;",
				"timestamp":        logTime2,
				"user":             "rdsadmin[rdsadmin]",
				"user_id":          "1",
			},
		},
		ParseAndEnhanceSpec{
			Title: "RDS Slowquery Log clever user",
			Line: `2017-04-05T21:57:46+00:00 aws-rds production-aurora-test-db: Slow query: # Time: 190921 16:02:59
# User@Host: clever[clever] @ [10.1.11.112] Id: 868
# Query_time: 2.000270 Lock_time: 0.000000 Rows_sent: 1 Rows_examined: 0
SET timestamp=1571090308;
select sleep(2);`,
			ExpectedOutput: map[string]interface{}{
				"env":              "deploy-env",
				"hostname":         "aws-rds",
				"programname":      "production-aurora-test-db",
				"decoder_msg_type": "syslog",
				"rawlog":           "Slow query: # Time: 190921 16:02:59\n# User@Host: clever[clever] @ [10.1.11.112] Id: 868\n# Query_time: 2.000270 Lock_time: 0.000000 Rows_sent: 1 Rows_examined: 0\nSET timestamp=1571090308;\nselect sleep(2);",
				"timestamp":        logTime2,
				"user":             "clever[clever]",
				"user_id":          "868",
			},
		},
		{
			Title: "Valid fluent log",
			Line: `{
  "container_id": "b17752cdd47a480712eca6c9774d782f68b50a876446faec5e26c9e8846bd29e",
  "container_name": "/ecs-env--app--d--2-2-env--app-e295a2dcf6f9ac849101",
  "ecs_cluster": "arn:aws:ecs:us-east-1:589690932525:cluster/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1",
  "ecs_task_arn": "arn:aws:ecs:us-east-1:589690932525:task/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1/7c1ccb5cb5c44582808b9e516b479eb6",
  "ecs_task_definition": "env--app--d--2:2",
  "fluent_ts": "2020-08-13T21:10:57.000+0000",
  "log": "2020/08/13 21:10:57 {\"title\": \"request-finished\"}",
  "source": "stderr"
}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime4,
				"rawlog":           "2020/08/13 21:10:57 {\"title\": \"request-finished\"}",
				"hostname":         "aws-fargate",
				"decoder_msg_type": "Kayvee",
				"title":            "request-finished",
				"container_app":    "app",
				"container_env":    "env",
				"container_task":   "7c1ccb5cb5c44582808b9e516b479eb6",
				"prefix":           "2020/08/13 21:10:57 ",
				"postfix":          "",
				"env":              "deploy-env",
			},
			ExpectedError: nil,
		},
		{
			Title: "fluent log missing ecs_task_definition still parses and has container_task",
			Line: `{
  "container_id": "b17752cdd47a480712eca6c9774d782f68b50a876446faec5e26c9e8846bd29e",
  "container_name": "/ecs-env--app--d--2-2-env--app-e295a2dcf6f9ac849101",
  "ecs_cluster": "arn:aws:ecs:us-east-1:589690932525:cluster/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1",
  "ecs_task_arn": "arn:aws:ecs:us-east-1:589690932525:task/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1/7c1ccb5cb5c44582808b9e516b479eb6",
  "fluent_ts": "2020-08-13T21:10:57.000+0000",
  "log": "2020/08/13 21:10:57 {\"title\": \"request-finished\"}",
  "source": "stderr"
}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime4,
				"rawlog":           "2020/08/13 21:10:57 {\"title\": \"request-finished\"}",
				"hostname":         "aws-fargate",
				"decoder_msg_type": "Kayvee",
				"title":            "request-finished",
				"container_task":   "7c1ccb5cb5c44582808b9e516b479eb6",
				"prefix":           "2020/08/13 21:10:57 ",
				"postfix":          "",
				"env":              "deploy-env",
			},
		},
		{
			Title: "fluent log with bad ecs_task_definition format still parses and has container_task",
			Line: `{
  "container_id": "b17752cdd47a480712eca6c9774d782f68b50a876446faec5e26c9e8846bd29e",
  "container_name": "/ecs-env--app--d--2-2-env--app-e295a2dcf6f9ac849101",
  "ecs_cluster": "arn:aws:ecs:us-east-1:589690932525:cluster/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1",
  "ecs_task_arn": "arn:aws:ecs:us-east-1:589690932525:task/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1/7c1ccb5cb5c44582808b9e516b479eb6",
  "ecs_task_definition": "env--app",
  "fluent_ts": "2020-08-13T21:10:57.000+0000",
  "log": "2020/08/13 21:10:57 {\"title\": \"request-finished\"}",
  "source": "stderr"
}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime4,
				"rawlog":           "2020/08/13 21:10:57 {\"title\": \"request-finished\"}",
				"hostname":         "aws-fargate",
				"decoder_msg_type": "Kayvee",
				"title":            "request-finished",
				"container_task":   "7c1ccb5cb5c44582808b9e516b479eb6",
				"prefix":           "2020/08/13 21:10:57 ",
				"postfix":          "",
				"env":              "deploy-env",
			},
		},
		{
			Title: "fluent log missing ecs_task_arn still parses and has containter_env and app",
			Line: `{
  "container_id": "b17752cdd47a480712eca6c9774d782f68b50a876446faec5e26c9e8846bd29e",
  "container_name": "/ecs-env--app--d--2-2-env--app-e295a2dcf6f9ac849101",
  "ecs_cluster": "arn:aws:ecs:us-east-1:589690932525:cluster/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1",
  "ecs_task_definition": "env--app--d--2:2",
  "fluent_ts": "2020-08-13T21:10:57.000+0000",
  "log": "2020/08/13 21:10:57 {\"title\": \"request-finished\"}",
  "source": "stderr"
}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime4, // time.Date(2020, 8, 13, 21, 10, 57, 0, time.UTC),
				"rawlog":           "2020/08/13 21:10:57 {\"title\": \"request-finished\"}",
				"hostname":         "aws-fargate",
				"decoder_msg_type": "Kayvee",
				"title":            "request-finished",
				"container_env":    "env",
				"container_app":    "app",
				"prefix":           "2020/08/13 21:10:57 ",
				"postfix":          "",
				"env":              "deploy-env",
			},
		},
		{
			Title: "fluent log missing timestamp is an error",
			Line: `{
  "container_id": "b17752cdd47a480712eca6c9774d782f68b50a876446faec5e26c9e8846bd29e",
  "container_name": "/ecs-env--app--d--2-2-env--app-e295a2dcf6f9ac849101",
  "ecs_cluster": "arn:aws:ecs:us-east-1:589690932525:cluster/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1",
  "ecs_task_arn": "arn:aws:ecs:us-east-1:589690932525:task/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1/7c1ccb5cb5c44582808b9e516b479eb6",
  "ecs_task_definition": "env--app--d--2:2",
  "ts": "2020-08-13T21:10:57.000+0000",
  "log": "2020/08/13 21:10:57 {\"deploy_env\":\"env\",\"level\":\"info\",\"pod-account\":\"589690932525\",\"pod-id\":\"2870425d-e4b1-4869-b2b7-a6d9fade9be1\",\"pod-region\":\"us-east-1\",\"pod-shortname\":\"us-east-1-dev-canary-2870425d\",\"source\":\"app-service\",\"title\":\"NumFDs\",\"type\":\"gauge\",\"value\":33,\"via\":\"process-metrics\"}",
  "source": "stderr"
}`,
			ExpectedOutput: nil,
			ExpectedError:  BadLogFormatError{},
		},
		{
			Title: "fluent log with bad timestamp format is an error",
			Line: `{
  "container_id": "b17752cdd47a480712eca6c9774d782f68b50a876446faec5e26c9e8846bd29e",
  "container_name": "/ecs-env--app--d--2-2-env--app-e295a2dcf6f9ac849101",
  "ecs_cluster": "arn:aws:ecs:us-east-1:589690932525:cluster/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1",
  "ecs_task_arn": "arn:aws:ecs:us-east-1:589690932525:task/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1/7c1ccb5cb5c44582808b9e516b479eb6",
  "ecs_task_definition": "env--app--d--2:2",
  "fluent_ts": "2020-08-13T21:10:57.000Z",
  "log": "2020/08/13 21:10:57 {\"deploy_env\":\"env\",\"level\":\"info\",\"pod-account\":\"589690932525\",\"pod-id\":\"2870425d-e4b1-4869-b2b7-a6d9fade9be1\",\"pod-region\":\"us-east-1\",\"pod-shortname\":\"us-east-1-dev-canary-2870425d\",\"source\":\"app-service\",\"title\":\"NumFDs\",\"type\":\"gauge\",\"value\":33,\"via\":\"process-metrics\"}",
  "source": "stderr"
}`,
			ExpectedOutput: nil,
			ExpectedError:  BadLogFormatError{},
		},
		{
			Title: "Valid fluent log with overrides for container_env, app, and task",
			Line: `{
  "container_id": "b17752cdd47a480712eca6c9774d782f68b50a876446faec5e26c9e8846bd29e",
  "container_name": "/ecs-env--app--d--2-2-env--app-e295a2dcf6f9ac849101",
  "ecs_cluster": "arn:aws:ecs:us-east-1:589690932525:cluster/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1",
  "ecs_task_arn": "arn:aws:ecs:us-east-1:589690932525:task/pod-2870425d-e4b1-4869-b2b7-a6d9fade9be1/7c1ccb5cb5c44582808b9e516b479eb6",
  "ecs_task_definition": "env--app--d--2:2",
  "fluent_ts": "2020-08-13T21:10:57.000+0000",
  "log": "2020/08/13 21:10:57 {\"title\": \"request-finished\", \"container_env\": \"env2\", \"container_task\": \"task\", \"container_app\": \"app2\"}",
  "source": "stderr"
}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":        logTime4,
				"rawlog":           "2020/08/13 21:10:57 {\"title\": \"request-finished\", \"container_env\": \"env2\", \"container_task\": \"task\", \"container_app\": \"app2\"}",
				"hostname":         "aws-fargate",
				"decoder_msg_type": "Kayvee",
				"title":            "request-finished",
				"container_app":    "app2",
				"container_env":    "env2",
				"container_task":   "task",
				"prefix":           "2020/08/13 21:10:57 ",
				"postfix":          "",
				"env":              "deploy-env",
			},
			ExpectedError: nil,
		},
	}
	for _, spec := range specs {
		t.Run(fmt.Sprint(spec.Title), func(t *testing.T) {
			assert := assert.New(t)
			fields, err := ParseAndEnhance(spec.Line, "deploy-env")
			if spec.ExpectedError != nil {
				assert.Error(err)
				assert.True(errors.As(err, &spec.ExpectedError))
				return
			}
			assert.NoError(err)
			assert.Equal(spec.ExpectedOutput, fields)
		})
	}
}

func TestGetContainerMeta(t *testing.T) {

	type containerMetaSpec struct {
		description       string
		input             map[string]interface{}
		wantErr           bool
		wantContainerEnv  string
		wantContainerApp  string
		wantContainerTask string
	}

	tests := []containerMetaSpec{
		{
			description: "Must have a programname to get container meta",
			input: map[string]interface{}{
				"programname": "",
			},
			wantErr: true,
		},
		{
			description: "Can parse a programname",
			input: map[string]interface{}{
				"programname": `env1--app2/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
			},
			wantContainerEnv:  "env1",
			wantContainerApp:  "app2",
			wantContainerTask: "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
		},
		{
			description: "Can override just 'env'",
			input: map[string]interface{}{
				"programname":   `env1--app2/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"container_env": "force-env",
			},
			wantContainerEnv:  "force-env",
			wantContainerApp:  "app2",
			wantContainerTask: "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
		},
		{
			description: "Can override just 'app'",
			input: map[string]interface{}{
				"programname":   `env1--app2/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"container_app": "force-app",
			},
			wantContainerEnv:  "env1",
			wantContainerApp:  "force-app",
			wantContainerTask: "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
		},
		{
			description: "Can override just 'task'",
			input: map[string]interface{}{
				"programname":    `env1--app2/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"container_task": "force-task",
			},
			wantContainerEnv:  "env1",
			wantContainerApp:  "app2",
			wantContainerTask: "force-task",
		},
		{
			description: "Can override all fields",
			input: map[string]interface{}{
				"programname":    `env1--app2/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"container_task": "force-task",
				"container_app":  "force-app",
				"container_env":  "force-env",
			},
			wantContainerEnv:  "force-env",
			wantContainerApp:  "force-app",
			wantContainerTask: "force-task",
		},
	}

	for _, tcase := range tests {
		assert := assert.New(t)

		syslog := map[string]interface{}{}
		for k, v := range tcase.input {
			syslog[k] = v
		}

		newSyslog, err := addContainterMetaToSyslog(syslog)
		if tcase.wantErr {
			assert.Error(err)
			continue
		}
		for k, v := range newSyslog {
			switch k {
			case "container_env":
				assert.Equal(v, tcase.wantContainerEnv)
			case "container_app":
				assert.Equal(v, tcase.wantContainerApp)
			case "container_task":
				assert.Equal(v, tcase.wantContainerTask)
			default:
				assert.Equal(v, tcase.input[k])
			}
		}
	}

}

func TestExtractKVMeta(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		Description                string
		Log                        map[string]interface{}
		Team                       string
		Language                   string
		Version                    string
		Names                      []string
		ExpectedMetricsRoutes      []MetricsRoute
		ExpectedAnalyticsRoutes    []AnalyticsRoute
		ExpectedNotificationRoutes []NotificationRoute
		ExpectedAlertRoutes        []AlertRoute
	}{
		{
			Description: "log line with no routes",
			Names:       []string{},
			Log:         map[string]interface{}{"hi": "hello!"},
		},
		{
			Description: "empty _kvmeta",
			Names:       []string{},
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
			Names:       []string{},
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
			Names:       []string{"cool", "cool2"},
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
					"routes": []interface{}{
						map[string]interface{}{
							"type":        "metrics",
							"rule":        "cool",
							"series":      "1,1,2,3,5,8,13",
							"value_field": "value",
							"dimensions":  []interface{}{"app", "district"},
						},
						map[string]interface{}{
							"type":       "metrics",
							"rule":       "cool2",
							"series":     "1,1,2,6,24,120,720,5040",
							"dimensions": []interface{}{"app", "district"},
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
			Names:       []string{"what's-this?", "there's-app-invites-everywhere"},
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
					"routes": []interface{}{
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
			Names:       []string{"did-you-know", "what's-the-catch"},
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
					"routes": []interface{}{
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
			Names:       []string{"last-call", "watch-out-now"},
			ExpectedAlertRoutes: []AlertRoute{
				{
					RuleName:   "last-call",
					Series:     "doing-it-til-we-fall",
					Dimensions: []string{"who", "where"},
					StatType:   "gauge",
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
					"routes": []interface{}{
						map[string]interface{}{
							"type":        "alerts",
							"rule":        "last-call",
							"series":      "doing-it-til-we-fall",
							"dimensions":  []interface{}{"who", "where"},
							"stat_type":   "gauge",
							"value_field": "status",
						},
						map[string]interface{}{
							"type":       "alerts",
							"rule":       "watch-out-now",
							"series":     "dem-gators-gonna-bite-ya",
							"dimensions": []interface{}{"how-fresh", "how-clean"},
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
			Names: []string{
				"all-combos", "there's-app-invites-everywhere", "what's-the-catch", "last-call",
			},
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
					StatType:   "gauge",
					ValueField: "status",
				},
			},
			Log: map[string]interface{}{
				"hi": "hello!",
				"_kvmeta": map[string]interface{}{
					"team":        "diversity",
					"kv_version":  "kv-routes",
					"kv_language": "understanding",
					"routes": []interface{}{
						map[string]interface{}{
							"type":       "metrics",
							"rule":       "all-combos",
							"series":     "1,1,2,6,24,120,720,5040",
							"dimensions": []interface{}{"fact", "orial"},
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
							"dimensions":  []interface{}{"who", "where"},
							"stat_type":   "gauge",
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

		assert.EqualValues(
			sort.StringSlice(test.Names), sort.StringSlice(kvmeta.Routes.RuleNames()),
		)

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
		_, err := ParseAndEnhance(benchmarkLine, "env")
		if err != nil {
			b.FailNow()
		}
	}
}
