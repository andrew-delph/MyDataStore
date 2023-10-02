package main

import (
	"container/heap"
	"sync"

	"github.com/sirupsen/logrus"
)

type ConsistencyItem struct {
	PartitionId int
	Epoch       int64
	SyncTask    bool
	Attemps     int
}

type ConsistencyHeap struct {
	queue []ConsistencyItem
	mu    sync.Mutex
	cond  *sync.Cond
}

func NewConsistencyHeap() *ConsistencyHeap {
	h := &ConsistencyHeap{}
	h.cond = sync.NewCond(&h.mu)
	return h
}

func (h *ConsistencyHeap) Len() int {
	return len(h.queue)
}

func (h *ConsistencyHeap) Less(i, j int) bool {
	// Min-heap comparison
	a := h.queue[i]
	b := h.queue[j]
	if a.Attemps != b.Attemps {
		return a.Attemps < b.Attemps
	}
	if a.SyncTask && b.SyncTask {
		return a.Epoch > b.Epoch
	} else if a.SyncTask || b.SyncTask {
		return a.SyncTask
	} else {
		return a.Epoch > b.Epoch
	}
}

func (h *ConsistencyHeap) Swap(i, j int) {
	h.queue[i], h.queue[j] = h.queue[j], h.queue[i]
}

func (h *ConsistencyHeap) Push(x interface{}) {
	h.queue = append(h.queue, x.(ConsistencyItem))
}

func (h *ConsistencyHeap) Pop() interface{} {
	n := len(h.queue)
	x := h.queue[n-1]
	h.queue = h.queue[0 : n-1]
	return x
}

func (h *ConsistencyHeap) PushSyncTask(PartitionId int, Epoch int64) {
	h.PushItem(ConsistencyItem{PartitionId: PartitionId, Epoch: Epoch, SyncTask: true})
}

func (h *ConsistencyHeap) PushVerifyTask(PartitionId int, Epoch int64) {
	h.PushItem(ConsistencyItem{PartitionId: PartitionId, Epoch: Epoch, SyncTask: false})
}

func (h *ConsistencyHeap) ManualPush(PartitionId int, Epoch int64, SyncTask bool, Attemps int) {
	h.PushItem(ConsistencyItem{PartitionId: PartitionId, Epoch: Epoch, SyncTask: SyncTask, Attemps: Attemps})
}

func (h *ConsistencyHeap) RequeueItem(item ConsistencyItem) {
	item.Attemps++
	if item.Attemps > 3 {
		logrus.Warnf("requeue item: %+v", item)
	}
	heap.Push(h, item)
}

func (h *ConsistencyHeap) PopItem() ConsistencyItem {
	h.mu.Lock()
	for len(h.queue) == 0 {
		h.cond.Wait()
	}
	defer h.mu.Unlock()
	item := heap.Pop(h)
	return item.(ConsistencyItem)
}

func (h *ConsistencyHeap) PushItem(item ConsistencyItem) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cond.Signal()
	heap.Push(h, item)
}
