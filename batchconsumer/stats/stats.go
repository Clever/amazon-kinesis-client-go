package stats

import (
	"time"

	"gopkg.in/Clever/kayvee-go.v6/logger"
)

var log = logger.New("amazon-kinesis-client-go")

type datum struct {
	name     string
	value    int
	category string
}

var queue = make(chan datum, 1000)

func init() {
	data := map[string]int{}
	tick := time.Tick(time.Minute)
	go func() {
		for {
			select {
			case d := <-queue:
				if d.category == "counter" {
					data[d.name] = data[d.name] + d.value
				} else if d.category == "gauge" {
					data[d.name] = d.value
				} else {
					log.ErrorD("unknow-stat-category", logger.M{"category": d.category})
				}
			case <-tick:
				tmp := logger.M{}
				for k, v := range data {
					tmp[k] = v
				}
				log.InfoD("stats", tmp)
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
