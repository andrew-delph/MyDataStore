package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	healthyPartitionsGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "node_partitions_healthy",
			Help: "The number of healthy partitions for a node",
		},
		[]string{"hostname"},
	)

	unhealthyPartitionsGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "node_partitions_unhealthy",
			Help: "The number of unhealthy partitions for a node",
		},
		[]string{"hostname"},
	)

	andrewGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "andrewGauge",
			Help: "andrewGauge is for testing...",
		},
		[]string{"hostname"},
	)
)
