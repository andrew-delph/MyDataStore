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
		1: 3, 2: 4, 3: 2,
	}

	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	pq := &PartitionEpochQueue{}
	heap.Init(pq)
	for partitionId, epoch := range items {
		pq.PushItem(&PartitionEpochItem{
			epoch:       int64(epoch),
			partitionId: partitionId,
		})
	}

	// Insert a new item and then modify its priority.
	item := &PartitionEpochItem{
		epoch:       1,
		partitionId: 7,
	}
	pq.PushItem(item)

	item = &PartitionEpochItem{
		epoch:       6,
		partitionId: 22,
	}
	pq.PushItem(item)

	popped := pq.PopItem()

	assert.EqualValues(t, 1, popped.epoch, "popped.epoch wrong value")
	assert.EqualValues(t, 7, popped.partitionId, "popped.partitionId wrong value")

	// Take the items out; they arrive in decreasing priority order.
	for pq.Len() > 0 {
		item := pq.PopItem()
		logrus.Infof("partitionId = %d epoch = %d", item.partitionId, item.epoch)
	}
}
