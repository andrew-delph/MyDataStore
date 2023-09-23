package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	partitionVerifyEpochAttemptsGague = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "partition_verify_epoch_attempts",
			Help: "the number of attempts to verify an epoch",
		},
		[]string{"partitionId", "epoch"},
	)

	partitionValidEpochGague = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "partition_valid_epoch",
			Help: "the last epoch of a partition",
		},
		[]string{"partitionId", "epoch"},
	)

	andrewGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "andrewGauge",
			Help: "andrewGauge is for testing...",
		},
		[]string{"hostname"},
	)
)

var (
	partitionsGained         prometheus.Counter
	partitionsLost           prometheus.Counter
	partitionsTotal          prometheus.Gauge
	healthyPartitionsGauge   prometheus.Gauge
	unhealthyPartitionsGauge prometheus.Gauge
	partitionActive          prometheus.GaugeVec
)

func initMetrics(hostname string) {
	constantLabels := prometheus.Labels{"hostname": hostname}
	partitionsLost = promauto.NewCounter(
		prometheus.CounterOpts{
			Name:        "partitions_lost",
			Help:        "number of partitions lost",
			ConstLabels: constantLabels,
		},
	)

	partitionsGained = promauto.NewCounter(
		prometheus.CounterOpts{
			Name:        "partitions_gained",
			Help:        "number of partitions gained",
			ConstLabels: constantLabels,
		},
	)

	partitionsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name:        "partitions_total",
			Help:        "number of partitions gained",
			ConstLabels: constantLabels,
		},
	)

	healthyPartitionsGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name:        "node_partitions_healthy",
			Help:        "The number of healthy partitions for a node",
			ConstLabels: constantLabels,
		},
	)

	unhealthyPartitionsGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name:        "node_partitions_unhealthy",
			Help:        "The number of unhealthy partitions for a node",
			ConstLabels: constantLabels,
		},
	)

	partitionActive = *promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "partition_active",
			Help:        "If a partition is active on a node",
			ConstLabels: constantLabels,
		},
		[]string{"partitionId"},
	)
}
