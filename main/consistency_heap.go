package main

import (
	"container/heap"
	"sync"
)

type ConsistencyItem struct {
	Epoch    int64
	SyncTask bool
	mu       sync.Mutex
	cond     *sync.Cond
}

// IntHeap is a slice of ints that implements heap.Interface to form a min-heap.
type ConsistencyHeap []ConsistencyItem

func NewConsistencyHeap() *ConsistencyHeap {
	h := &ConsistencyHeap{}
	return h
}

func (h ConsistencyHeap) Len() int {
	return len(h)
}

func (h ConsistencyHeap) Less(i, j int) bool {
	// Min-heap comparison
	a := h[i]
	b := h[j]
	if a.SyncTask && b.SyncTask {
		return a.Epoch > b.Epoch
	} else if a.SyncTask || b.SyncTask {
		return !a.SyncTask
	} else {
		return a.Epoch > b.Epoch
	}
}

func (h ConsistencyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ConsistencyHeap) Push(x interface{}) {
	*h = append(*h, x.(ConsistencyItem))
}

func (h *ConsistencyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *ConsistencyHeap) PushSyncTask(Epoch int64) {
	heap.Push(h, ConsistencyItem{Epoch: Epoch, SyncTask: true})
}

func (h *ConsistencyHeap) PushVerifyTask(Epoch int64) {
	heap.Push(h, ConsistencyItem{Epoch: Epoch, SyncTask: false})
}
