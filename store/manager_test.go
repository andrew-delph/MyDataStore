package main

import (
	"container/heap"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestPartitionVerifyQueue(t *testing.T) {
	// Some items and their priorities.
	items := map[int]int{
		1: 3, 2: 2, 3: 1,
	}

	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	pq := make(PartitionEpochQueue, len(items))
	i := 0
	for partitionId, epoch := range items {
		pq[i] = &PartitionEpochItem{
			epoch:       int64(epoch),
			partitionId: partitionId,
		}
		i++
	}
	heap.Init(&pq)

	// Insert a new item and then modify its priority.
	item := &PartitionEpochItem{
		epoch:       6,
		partitionId: 6,
	}
	pq.Push(item)
	// heap.Push(&pq, item)

	popped := heap.Pop(&pq).(*PartitionEpochItem)

	assert.EqualValues(t, 1, popped.epoch, "popped.epoch wrong value")
	assert.EqualValues(t, 3, popped.partitionId, "popped.partitionId wrong value")

	// Take the items out; they arrive in decreasing priority order.
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*PartitionEpochItem)
		logrus.Infof("partitionId = %d epoch = %d", item.partitionId, item.epoch)
	}
}
