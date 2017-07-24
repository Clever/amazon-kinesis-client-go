package decode

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Clever/syslogparser/rfc3164"
)

// reservedFields are automatically set during decoding.
// no field written by a user (e.g. contained in the Kayvee JSON) should overwrite them.
var reservedFields = []string{
	"prefix",
	"postfix",
	"Type",
}

func stringInSlice(s string, slice []string) bool {
	for _, item := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// remapSyslog3164Keys renames fields to match our expecations from heka's syslog parser
// see: https://github.com/mozilla-services/heka/blob/278dd3d5961b9b6e47bb7a912b63ce3faaf8d8bd/sandbox/lua/decoders/rsyslog.lua
var remapSyslog3164Keys = map[string]string{
	"hostname":  "hostname",
	"timestamp": "timestamp",
	"tag":       "programname",
	"content":   "rawlog",
}

// FieldsFromSyslog takes an RSyslog formatted log line and extracts fields from it
//
// Supports two log lines formats:
// - RSYSLOG_TraditionalFileFormat - the "old style" default log file format with low-precision timestamps (RFC3164)
// - RSYSLOG_FileFormat - a modern-style logfile format similar to TraditionalFileFormat, but with high-precision timestamps and timezone information
//
// For more details on Rsylog formats: https://rsyslog-5-8-6-doc.neocities.org/rsyslog_conf_templates.html
func FieldsFromSyslog(line string) (map[string]interface{}, error) {
	// rfc3164 includes a severity number in front of the Syslog line, but we don't use that
	fakeSeverity := "<12>"
	p3164 := rfc3164.NewParser([]byte(fakeSeverity + line))
	err := p3164.Parse()
	if err != nil {
		return map[string]interface{}{}, err
	}

	out := map[string]interface{}{}
	for k, v := range p3164.Dump() {
		if newKey, ok := remapSyslog3164Keys[k]; ok {
			out[newKey] = v
		}
	}
	return out, nil
}

// NonKayveeError occurs when the log line is not Kayvee
type NonKayveeError struct{}

func (e NonKayveeError) Error() string {
	return fmt.Sprint("Log line is not Kayvee (doesn't have JSON payload)")
}

// FieldsFromKayvee takes a log line and extracts fields from the Kayvee (JSON) part
func FieldsFromKayvee(line string) (map[string]interface{}, error) {
	m := map[string]interface{}{}

	firstIdx := strings.Index(line, "{")
	lastIdx := strings.LastIndex(line, "}")
	if firstIdx == -1 || lastIdx == -1 || firstIdx > lastIdx {
		return map[string]interface{}{}, &NonKayveeError{}
	}
	m["prefix"] = line[:firstIdx]
	m["postfix"] = line[lastIdx+1:]

	possibleJSON := line[firstIdx : lastIdx+1]
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(possibleJSON), &fields); err != nil {
		return map[string]interface{}{}, err
	}
	for k, v := range fields {
		if !stringInSlice(k, reservedFields) {
			m[k] = v
		}
	}

	m["type"] = "Kayvee"

	return m, nil
}

// MetricsRoute represents a metrics kv log route
type MetricsRoute struct {
	Series     string
	Dimensions []string
	ValueField string
	RuleName   string
}

// AnalyticsRoute represents an analytics kv log route
type AnalyticsRoute struct {
	Series   string
	RuleName string
}

// NotificationRoute represents a notification kv log route
type NotificationRoute struct {
	Channel  string
	Icon     string
	Message  string
	User     string
	RuleName string
}

// AlertRoute represents an alert kv log route
type AlertRoute struct {
	Series     string
	Dimensions []string
	StatType   string
	ValueField string
	RuleName   string
}

func getStringValue(json map[string]interface{}, key string) string {
	val, ok := json[key]
	if !ok {
		return ""
	}

	str, ok := val.(string)
	if !ok {
		return ""
	}

	return str
}

func getStringArray(json map[string]interface{}, key string) []string {
	val, ok := json[key]
	if !ok {
		return []string{}
	}

	strArray, ok := val.([]string)
	if !ok {
		return []string{}
	}

	return strArray
}

// LogRoutes a type alias to make it easier to add route specific filter functions
type LogRoutes []map[string]interface{}

// MetricsRoutes filters the LogRoutes and returns a list of MetricsRoutes structs
func (l LogRoutes) MetricsRoutes() []MetricsRoute {
	routes := []MetricsRoute{}

	for _, route := range l {
		tipe := getStringValue(route, "type")
		if tipe != "metrics" {
			continue
		}

		series := getStringValue(route, "series")
		dimensions := getStringArray(route, "dimensions")
		valueField := getStringValue(route, "value_field")
		ruleName := getStringValue(route, "rule")

		if series == "" { // TODO: log error
			continue
		}
		if valueField == "" {
			valueField = "value"
		}

		routes = append(routes, MetricsRoute{
			Series:     series,
			Dimensions: dimensions,
			ValueField: valueField,
			RuleName:   ruleName,
		})
	}

	return routes
}

// AnalyticsRoutes filters the LogRoutes and returns a list of AnalyticsRoutes structs
func (l LogRoutes) AnalyticsRoutes() []AnalyticsRoute {
	routes := []AnalyticsRoute{}

	for _, route := range l {
		tipe := getStringValue(route, "type")
		if tipe != "analytics" {
			continue
		}

		series := getStringValue(route, "series")
		ruleName := getStringValue(route, "rule")

		if series == "" { // TODO: log error
			continue
		}

		routes = append(routes, AnalyticsRoute{
			Series:   series,
			RuleName: ruleName,
		})
	}

	return routes
}

// NotificationRoutes filters the LogRoutes and returns a list of NotificationRoutes structs
func (l LogRoutes) NotificationRoutes() []NotificationRoute {
	routes := []NotificationRoute{}

	for _, route := range l {
		tipe := getStringValue(route, "type")
		if tipe != "notifications" {
			continue
		}

		channel := getStringValue(route, "channel")
		icon := getStringValue(route, "icon")
		message := getStringValue(route, "message")
		user := getStringValue(route, "user")
		rule := getStringValue(route, "rule")

		if channel == "" || message == "" { // TODO: log error
			continue
		}

		if icon == "" {
			icon = ":ghost:"
		}
		if user == "" {
			user = "logging-pipeline"
		}

		routes = append(routes, NotificationRoute{
			Channel:  channel,
			Icon:     icon,
			Message:  message,
			User:     user,
			RuleName: rule,
		})
	}

	return routes
}

// AlertRoutes filters the LogRoutes and returns a list of AlertRoutes structs
func (l LogRoutes) AlertRoutes() []AlertRoute {
	routes := []AlertRoute{}

	for _, route := range l {
		tipe := getStringValue(route, "type")
		if tipe != "alerts" {
			continue
		}

		series := getStringValue(route, "series")
		dimensions := getStringArray(route, "dimensions")
		statType := getStringValue(route, "stat_type")
		valueField := getStringValue(route, "value_field")
		ruleName := getStringValue(route, "rule")

		if series == "" { // TODO: log error
			continue
		}
		if statType == "" {
			statType = "counter"
		}
		if valueField == "" {
			valueField = "value"
		}

		routes = append(routes, AlertRoute{
			Series:     series,
			Dimensions: dimensions,
			StatType:   statType,
			ValueField: valueField,
			RuleName:   ruleName,
		})
	}

	return routes
}

// KVMeta a struct that represents kv-meta data
type KVMeta struct {
	Team     string
	Version  string
	Language string
	Routes   LogRoutes
}

// ExtractKVMeta returns a struct with available kv-meta data
func ExtractKVMeta(kvlog map[string]interface{}) KVMeta {
	tmp, ok := kvlog["_kvmeta"]
	if !ok {
		return KVMeta{}
	}

	kvmeta, ok := tmp.(map[string]interface{})
	if !ok {
		return KVMeta{}
	}

	kvRoutes := []map[string]interface{}{}

	tmp, ok = kvmeta["routes"]
	if ok {
		routes, ok := tmp.([]map[string]interface{})
		if ok {
			kvRoutes = routes
		}
	}

	return KVMeta{
		Team:     getStringValue(kvmeta, "team"),
		Version:  getStringValue(kvmeta, "kv_version"),
		Language: getStringValue(kvmeta, "kv_language"),
		Routes:   kvRoutes,
	}
}

// ParseAndEnhance extracts fields from a log line, and does some post-processing to rename/add fields
func ParseAndEnhance(line string, env string, stringifyNested bool, renameESReservedFields bool, minimumTimestamp time.Time) (map[string]interface{}, error) {
	out := map[string]interface{}{}

	syslogFields, err := FieldsFromSyslog(line)
	if err != nil {
		return map[string]interface{}{}, err
	}
	for k, v := range syslogFields {
		out[k] = v
	}
	rawlog := syslogFields["rawlog"].(string)
	programname := syslogFields["programname"].(string)

	// Try pulling Kayvee fields out of message
	kvFields, err := FieldsFromKayvee(rawlog)
	if err != nil {
		if _, ok := err.(*NonKayveeError); !ok {
			return map[string]interface{}{}, err
		}
	} else {
		for k, v := range kvFields {
			out[k] = v
		}
	}

	// Inject additional fields that are useful in log-searching and other business logic
	out["env"] = env

	// Sometimes its useful to force `container_{env,app,task}`. A specific use-case is writing Docker events.
	// A separate container monitors for start/stop events, but we set the container values in such a way that
	// the logs for these events will appear in context for the app that the user is looking at instead of the
	// docker-events app.
	forceEnv := ""
	forceApp := ""
	forceTask := ""
	if cEnv, ok := out["container_env"]; ok {
		forceEnv = cEnv.(string)
	}
	if cApp, ok := out["container_app"]; ok {
		forceApp = cApp.(string)
	}
	if cTask, ok := out["container_task"]; ok {
		forceTask = cTask.(string)
	}
	meta, err := getContainerMeta(programname, forceEnv, forceApp, forceTask)
	if err == nil {
		for k, v := range meta {
			out[k] = v
		}
	}

	// ES dynamic mappings get finnicky once you start sending nested objects.
	// E.g., if one app sends a field for the first time as an object, then any log
	// sent by another app containing that field /not/ as an object will fail.
	// One solution is to decode nested objects as strings.
	if stringifyNested {
		for k, v := range out {
			_, ismap := v.(map[string]interface{})
			_, isarr := v.([]interface{})
			if ismap || isarr {
				bs, _ := json.Marshal(v)
				out[k] = string(bs)
			}
		}
	}

	// ES doesn't like fields that start with underscores.
	if renameESReservedFields {
		for oldKey, renamedKey := range esFieldRenames {
			if val, ok := out[oldKey]; ok {
				out[renamedKey] = val
				delete(out, oldKey)
			}
		}
	}

	msgTime, ok := out["timestamp"].(time.Time)
	if ok && !msgTime.After(minimumTimestamp) {
		return map[string]interface{}{}, fmt.Errorf("message's timestamp < minimumTimestamp")
	}

	return out, nil
}

var esFieldRenames = map[string]string{
	"_index":       "kv__index",
	"_uid":         "kv__uid",
	"_type":        "kv__type",
	"_id":          "kv__id",
	"_source":      "kv__source",
	"_size":        "kv__size",
	"_all":         "kv__all",
	"_field_names": "kv__field_names",
	"_timestamp":   "kv__timestamp",
	"_ttl":         "kv__ttl",
	"_parent":      "kv__parent",
	"_routing":     "kv__routing",
	"_meta":        "kv__meta",
}

const containerMeta = `([a-z0-9-]+)--([a-z0-9-]+)\/` + // env--app
	`arn%3Aaws%3Aecs%3Aus-(west|east)-[1-2]%3A[0-9]{12}%3Atask%2F` + // ARN cruft
	`([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})` // task-id

var containerMetaRegex = regexp.MustCompile(containerMeta)

func getContainerMeta(programname, forceEnv, forceApp, forceTask string) (map[string]string, error) {
	if programname == "" {
		return map[string]string{}, fmt.Errorf("no programname")
	}

	env := ""
	app := ""
	task := ""
	matches := containerMetaRegex.FindAllStringSubmatch(programname, 1)
	if len(matches) == 1 {
		env = matches[0][1]
		app = matches[0][2]
		task = matches[0][4]
	}

	if forceEnv != "" {
		env = forceEnv
	}
	if forceApp != "" {
		app = forceApp
	}
	if forceTask != "" {
		task = forceTask
	}

	if env == "" || app == "" || task == "" {
		return map[string]string{}, fmt.Errorf("unable to get one or more of env/app/task")
	}

	return map[string]string{
		"container_env":  env,
		"container_app":  app,
		"container_task": task,
	}, nil
}
