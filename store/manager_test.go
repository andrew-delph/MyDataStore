package main

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPartitionVerifyQueue(t *testing.T) {
	// Some items and their priorities.
	items := map[int]int{
		1:  3,
		22: 6,
		2:  4,
		7:  1,
		3:  2,
	}

	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	pq := &PartitionEpochQueue{}
	heap.Init(pq)

	var nilValue *PartitionEpochItem
	assert.Equal(t, nilValue, pq.PopItem(), "Should be nil")

	now := time.Now()

	pq.PushItem(&PartitionEpochItem{
		epoch:       int64(66),
		partitionId: 66,
	})

	pq.PushItem(&PartitionEpochItem{
		epoch:       int64(66),
		partitionId: 77,
		nextAttempt: now.Add(1 * time.Second),
	})
	pq.PushItem(&PartitionEpochItem{
		epoch:       int64(66),
		partitionId: 88,
		nextAttempt: now.Add(2 * time.Second),
	})

	peekedTime1 := pq.PopItem()
	assert.EqualValues(t, 66, peekedTime1.epoch, "peekedTime1.epoch wrong value")
	assert.EqualValues(t, 66, peekedTime1.partitionId, "peekedTime1.partitionId wrong value")

	peekedTime2 := pq.PopItem()
	assert.EqualValues(t, 66, peekedTime2.epoch, "peeked.epoch wrong value")
	assert.EqualValues(t, 77, peekedTime2.partitionId, "peeked.partitionId wrong value")

	peekedTime3 := pq.PopItem()
	assert.EqualValues(t, 66, peekedTime3.epoch, "peeked.epoch wrong value")
	assert.EqualValues(t, 88, peekedTime3.partitionId, "peeked.partitionId wrong value")

	for partitionId, epoch := range items {
		pq.PushItem(&PartitionEpochItem{
			epoch:       int64(epoch),
			partitionId: partitionId,
		})
	}

	next1 := pq.NextItem()
	assert.EqualValues(t, 1, next1.epoch, "next1.epoch wrong value")
	assert.EqualValues(t, 7, next1.partitionId, "next1.partitionId wrong value")

	next2 := pq.NextItem()
	assert.EqualValues(t, 1, next2.epoch, "next2.epoch wrong value")
	assert.EqualValues(t, 7, next2.partitionId, "next2.partitionId wrong value")
	next2.completed = true

	next3 := pq.NextItem()
	assert.EqualValues(t, 2, next3.epoch, "next3.epoch wrong value")
	assert.EqualValues(t, 3, next3.partitionId, "next3.partitionId wrong value")

	peeked := pq.PeekItem()
	assert.EqualValues(t, 2, peeked.epoch, "peeked.epoch wrong value")
	assert.EqualValues(t, 3, peeked.partitionId, "peeked.partitionId wrong value")

	popped := pq.PopItem()
	assert.EqualValues(t, 2, popped.epoch, "popped.epoch wrong value")
	assert.EqualValues(t, 3, popped.partitionId, "popped.partitionId wrong value")
}
