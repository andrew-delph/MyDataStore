package main

import (
	"container/heap"
	"sync"
)

type ConsistencyItem struct {
	Epoch    int64
	SyncTask bool
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
	if a.SyncTask && b.SyncTask {
		return a.Epoch > b.Epoch
	} else if a.SyncTask || b.SyncTask {
		return !a.SyncTask
	} else {
		return a.Epoch > b.Epoch
	}
}

func (h *ConsistencyHeap) Swap(i, j int) {
	h.queue[i], h.queue[j] = h.queue[j], h.queue[i]
}

func (h *ConsistencyHeap) Push(x interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.cond.Signal()
	h.queue = append(h.queue, x.(ConsistencyItem))
}

func (h *ConsistencyHeap) Pop() interface{} {
	h.mu.Lock()
	for len(h.queue) == 0 {
		h.cond.Wait()
	}
	defer h.mu.Unlock()
	n := len(h.queue)
	x := h.queue[n-1]
	h.queue = h.queue[0 : n-1]
	return x
}

func (h *ConsistencyHeap) PushSyncTask(Epoch int64) {
	heap.Push(h, ConsistencyItem{Epoch: Epoch, SyncTask: true})
}

func (h *ConsistencyHeap) PushVerifyTask(Epoch int64) {
	heap.Push(h, ConsistencyItem{Epoch: Epoch, SyncTask: false})
}

func (h *ConsistencyHeap) PopItem() ConsistencyItem {
	item := h.Pop()
	return item.(ConsistencyItem)
}
