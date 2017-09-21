package decode

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/Clever/syslogparser/rfc3164"
)

// reservedFields are automatically set during decoding.
// no field written by a user (e.g. contained in the Kayvee JSON) should overwrite them.
var reservedFields = []string{
	"prefix",
	"postfix",
	"decoder_msg_type",
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
		return map[string]interface{}{}, &NonKayveeError{}
	}

	if len(fields) == 0 { // Some logs superfluous "{}" in them.  They're not kayvee.
		return map[string]interface{}{}, &NonKayveeError{}
	}
	// TODO: consider also filter if they have source and title

	for k, v := range fields {
		if !stringInSlice(k, reservedFields) {
			m[k] = v
		}
	}

	m["decoder_msg_type"] = "Kayvee"

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

	iArray, ok := val.([]interface{})
	if !ok {
		return []string{}
	}

	strArray := []string{}
	for _, item := range iArray {
		s, ok := item.(string)
		if !ok {
			return []string{}
		}
		strArray = append(strArray, s)
	}

	return strArray
}

// LogRoutes a type alias to make it easier to add route specific filter functions
type LogRoutes []map[string]interface{}

// RuleNames returns a list of the names of the rules
func (l LogRoutes) RuleNames() []string {
	names := []string{}
	for _, route := range l {
		name := getStringValue(route, "rule")
		names = append(names, name)
	}

	return names
}

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
		routes, ok := tmp.([]interface{})
		if ok {
			for _, route := range routes {
				rule, ok := route.(map[string]interface{})
				if ok { // TODO: log error
					kvRoutes = append(kvRoutes, rule)
				}
			}
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
func ParseAndEnhance(line string, env string) (map[string]interface{}, error) {
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

	return out, nil
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
