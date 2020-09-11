package stats

import (
	"time"

	"gopkg.in/Clever/kayvee-go.v6/logger"
)

var log = logger.New("amazon-kinesis-client-go")

// DefaultCounters are core counters tracked by the batchconsumer
// These are stats we want to report on every tick, even if the values are zero
var DefaultCounters = []string{
	"batches-sent",
	"checkpoints-sent",
	"msg-batched",
	"batch-log-failures",
	"unknown-error",
	"processed-messages",
	"no-tags",
	"blank-tag",
}

type datum struct {
	name     string
	value    int
	category string
}

var queue = make(chan datum, 1000)

func init() {
	countData := map[string]int{}
	gaugeData := map[string]int{}
	tick := time.Tick(time.Minute)
	go func() {
		for {
			select {
			case d := <-queue:
				if d.category == "counter" {
					countData[d.name] = countData[d.name] + d.value
				} else if d.category == "gauge" {
					gaugeData[d.name] = d.value
				} else {
					log.ErrorD("unknown-stat-category", logger.M{"category": d.category})
				}
			case <-tick:
				tmp := logger.M{}
				for _, k := range DefaultCounters {
					tmp[k] = 0
				}
				for k, v := range countData {
					tmp[k] = v
				}
				for k, v := range gaugeData {
					tmp[k] = v
				}
				log.InfoD("stats", tmp)
				countData = map[string]int{}
			}
		}
	}()
}

func Counter(name string, val int) {
	queue <- datum{name, val, "counter"}
}

func Gauge(name string, val int) {
	queue <- datum{name, val, "gauge"}
}
