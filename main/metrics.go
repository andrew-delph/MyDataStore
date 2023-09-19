package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	partitionActiveGague = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "partition_active",
			Help: "If a partition is active on a node",
		},
		[]string{"partitionId"},
	)

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
