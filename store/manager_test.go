package main

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPartitionVerifyQueueBasic(t *testing.T) {
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

func TestPartitionVerifyQueueNextAttempt(t *testing.T) {
	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	pq := &PartitionEpochQueue{}
	heap.Init(pq)

	var nilValue *PartitionEpochItem
	assert.Equal(t, nilValue, pq.PopItem(), "Should be nil")

	now := time.Now()

	pq.PushItem(&PartitionEpochItem{
		epoch:       int64(1),
		partitionId: 1,
	})

	pq.PushItem(&PartitionEpochItem{
		epoch:       int64(1),
		partitionId: 2,
		nextAttempt: now.Add(-2 * time.Second),
	})

	pq.PushItem(&PartitionEpochItem{
		epoch:       int64(1),
		partitionId: 4,
		nextAttempt: now,
	})

	pq.PushItem(&PartitionEpochItem{
		epoch:       int64(1),
		partitionId: 5,
		nextAttempt: now.Add(10 * time.Second),
	})

	pq.PushItem(&PartitionEpochItem{
		epoch:       int64(1),
		partitionId: 3,
		nextAttempt: now.Add(-1 * time.Second),
	})

	var nextItem *PartitionEpochItem
	nextItem = pq.NextItem()
	nextItem.completed = true
	assert.EqualValues(t, 1, nextItem.epoch, "peekedTime1.epoch wrong value")
	assert.EqualValues(t, 1, nextItem.partitionId, "peekedTime1.partitionId wrong value")

	nextItem = pq.NextItem()
	nextItem.completed = true
	assert.EqualValues(t, 1, nextItem.epoch, "peeked.epoch wrong value")
	assert.EqualValues(t, 2, nextItem.partitionId, "peeked.partitionId wrong value")

	nextItem = pq.NextItem()
	nextItem.completed = true
	assert.EqualValues(t, 1, nextItem.epoch, "peeked.epoch wrong value")
	assert.EqualValues(t, 3, nextItem.partitionId, "peeked.partitionId wrong value")
	pq.NextItem()

	assert.EqualValues(t, 2, pq.Len(), "Len() wrong value")
	assert.EqualValues(t, 4, pq.PeekItem().partitionId, "peeked.partitionId wrong value")
	pq.PushItem(&PartitionEpochItem{
		epoch:       int64(1),
		partitionId: 5,
		nextAttempt: time.Now(),
	})
	nextItem = pq.NextItem()
	// try and mess up queue with time changes...
	nextItem.nextAttempt = time.Now().Add(1 * time.Second)
	assert.EqualValues(t, 1, nextItem.epoch, "peeked.epoch wrong value")
	assert.EqualValues(t, 4, nextItem.partitionId, "peeked.partitionId wrong value")
	assert.EqualValues(t, 4, pq.PeekItem().partitionId, "peeked.partitionId wrong value")

	pq.Fix(nextItem)

	assert.EqualValues(t, 5, pq.PeekItem().partitionId, "peeked.partitionId wrong value")
}
