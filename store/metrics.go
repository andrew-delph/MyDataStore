package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

var myCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "andrew",
		Help: "testing andrew metric",
	},
	[]string{"type"},
)

func initProm() {
	// Register the metric.
	prometheus.MustRegister(myCounter)

	myCounter.WithLabelValues("andrew_type").Inc()
	myCounter.WithLabelValues("andrew_type").Inc()
	myCounter.WithLabelValues("andrew_type").Inc()
}
