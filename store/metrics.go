package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var healthyPartitionsGauge = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "node_partitions_healthy",
		Help: "The number of healthy partitions for a node",
	},
	[]string{"hostname"},
)
