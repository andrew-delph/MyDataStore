package main

import (
	"container/heap"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/cbergoon/merkletree"
	"github.com/sirupsen/logrus"
)

var (
	partitionEpochSynced = make([]int64, partitionBuckets)
	validFSM             = false
)
var partitionEpochQueue PartitionEpochQueue

func managerInit() {
	partitionEpochQueue = make(PartitionEpochQueue, 0)
	heap.Init(&partitionEpochQueue)
	logrus.Warn("managerInit")
	run := true
	go func() {
		for run {
			select {
			case validFSMUpdate := <-validFSMObserver:
				if validFSMUpdate {
					validFSM = true
					if !validFSM {
						logrus.Warn("FSM is now valid.")
					}
				} else {
					if validFSM {
						logrus.Panic("validFSM became false")
					}
					validFSM = false
				}
			default:
				if partitionEpochQueue.Len() > 0 {
					handlePartitionEpochItem()
				}
			}
		}
	}()
}

// verify that the cached partition global merkletree is in sync with R nodes
func verifyPartitionGlobalOld(partitionId int) (*map[int32]struct{}, error) {
	return verifyPartitionOld(partitionId, currGlobalBucketEpoch, true)
}

// verify that the cached partition epoch merkletree is in sync with R nodes
func verifyPartitionEpochOld(partitionId int, epoch int64) (*map[int32]struct{}, error) {
	return verifyPartitionOld(partitionId, epoch, false)
}

func verifyPartitionOld(partitionId int, epoch int64, global bool) (*map[int32]struct{}, error) {
	nodes, err := GetClosestNForPartition(events.consistent, partitionId, N)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	unsyncedCh := make(chan *map[int32]struct{}, len(nodes))
	errorCh := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(addr string) {
			unsyncedBuckets, err := VerifyMerkleTree(addr, epoch, global, partitionId)

			if err != nil && err != io.EOF {
				logrus.Errorf("RecentEpochSync VerifyMerkleTree unsyncedBuckets = %v partitionId = %v err = %v ", unsyncedBuckets, partitionId, err)
				errorCh <- err
			} else {
				logrus.Debugf("RecentEpochSync VerifyMerkleTree unsyncedBuckets = %v ", unsyncedBuckets)
				unsyncedCh <- &unsyncedBuckets
			}
		}(node.String())
	}

	timeout := time.After(time.Second * 40)
	responseCount := 0
	var unsyncedBuckets *map[int32]struct{}
	for i := 0; i < len(nodes); i++ {
		select {
		case temp := <-unsyncedCh:
			if temp != nil && len(*temp) > 0 {
				unsyncedBuckets = temp
			} else {
				responseCount++
			}
		case err := <-errorCh:
			logrus.Errorf("errorCh: %v", err)
			_ = err // Handle error if necessary
		case <-timeout:
			return unsyncedBuckets, fmt.Errorf("timed out waiting for responses")
		}
		if responseCount >= R {
			return nil, nil
		}
	}
	return unsyncedBuckets, fmt.Errorf("not enough nodes verifyied.")
}

func syncPartition(partitionId int, requestBuckets []int32, lowerEpoch, upperEpoch int64) error {
	nodes, err := GetClosestNForPartition(events.consistent, partitionId, N)
	if err != nil {
		logrus.Error(err)
		return err
	}
	logrus.Warnf("StreamBuckets partitionId = %d lowerEpoch = %d upperEpoch = %d bucketsnum = %d", partitionId, lowerEpoch, upperEpoch, len(requestBuckets))
	var wg sync.WaitGroup

	for i := 0; i < len(nodes); i++ {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			StreamBuckets(nodes[rand.Intn(len(nodes))].String(), requestBuckets, lowerEpoch, upperEpoch, partitionId)
		}(nodes[i].String())
	}

	wg.Wait()
	return nil
}

// update the global merkletree bucket
// verify merkle trees are in sync with R nodes. (recent cache and global)
// if out of sync. it will call appropiate sync functions
func handleEpochUpdate(currEpoch int64) error {
	logrus.Debugf("currEpoch %d", currEpoch)
	defer logrus.Warnf("done handleEpochUpdate %d", currEpoch)
	err := UpdateGlobalBucket(currEpoch)
	if err != nil {
		logrus.Error(fmt.Errorf("UpdateGlobalBucket err = %v", err))
		return err
	}
	for partId := 0; partId < partitionCount; partId++ {
		var err error
		var unsyncedBuckets *map[int32]struct{}
		if partitionEpochSynced[partId] == currEpoch-2 {
			unsyncedBuckets, err = verifyPartitionEpochOld(partId, currEpoch-1)
		} else {
			logrus.Warnf("checking global")
			unsyncedBuckets, err = verifyPartitionGlobalOld(partId)
		}
		if unsyncedBuckets != nil && len(*unsyncedBuckets) > 0 {
			logrus.Error("failed to sync partion = %d err = %v", partId, err)
			var requestBuckets []int32
			for b := range *unsyncedBuckets {
				requestBuckets = append(requestBuckets, b)
			}
			syncPartition(partId, requestBuckets, partitionEpochSynced[partId], currEpoch-1)

		} else {
			partitionEpochSynced[partId] = currEpoch - 1
			logrus.Debugf("Success sync of epoch = %d", currEpoch-1)
		}
	}
	return nil
}

func handlePartitionEpochItem() {
	item := heap.Pop(&partitionEpochQueue).(*PartitionEpochItem)
	tree, _, err := RawPartitionMerkleTree(item.epoch, false, item.partitionId)
	if err != nil {
		logrus.Errorf("handlePartitionEpochItem err = %v", err)
		partitionEpochQueue.Push(item)
		return
	}
	unsyncedBuckets, err := verifyPartitionEpochTree(tree, item.partitionId, item.epoch)
	if (unsyncedBuckets != nil && len(unsyncedBuckets) > 0) || err != nil {
		partitionEpochQueue.Push(item)
		logrus.Error("failed to sync partion = %d epoch = %d err = %v", item.partitionId, item.epoch, err)
		var requestBuckets []int32
		for _, buckedId := range unsyncedBuckets {
			requestBuckets = append(requestBuckets, buckedId)
		}
		syncPartition(item.partitionId, requestBuckets, item.epoch-1, item.epoch)

	} else {
		logrus.Warnf("Success sync of epoch = %d", currEpoch-1)
	}
}

func verifyPartitionEpochTree(tree *merkletree.MerkleTree, partitionId int, epoch int64) ([]int32, error) {
	nodes, err := GetClosestNForPartition(events.consistent, int(partitionId), N)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	unsyncedCh := make(chan []int32, len(nodes))
	errorCh := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(addr string) {
			otherParitionEpochObject, err := GetParitionEpochObject(addr, epoch, partitionId)
			if err != nil {
				logrus.Error(err)
				errorCh <- err
			}
			otherTree, err := ParitionEpochObjectToMerkleTree(otherParitionEpochObject)
			if err != nil {
				logrus.Error(err)
				errorCh <- err
			}
			unsyncedBuckets := DifferentMerkleTreeBuckets(tree, otherTree)
			if unsyncedBuckets == nil {
				errorCh <- fmt.Errorf("unsyncedBuckets is nil")
			} else {
				unsyncedCh <- unsyncedBuckets
			}
		}(node.String())
	}

	timeout := time.After(time.Second * 40)
	responseCount := 0
	var unsyncedBuckets []int32
	for i := 0; i < len(nodes); i++ {
		select {
		case temp := <-unsyncedCh:
			if len(temp) > 0 {
				unsyncedBuckets = temp
			} else {
				responseCount++
			}
		case err := <-errorCh:
			logrus.Errorf("errorCh: %v", err)
			_ = err // Handle error if necessary
		case <-timeout:
			return unsyncedBuckets, fmt.Errorf("timed out waiting for responses")
		}
		if responseCount >= R {
			return nil, nil
		}
	}
	return unsyncedBuckets, fmt.Errorf("not enough nodes verifyied.")
}

// An PartitionEpochItem is something we manage in a priority queue.
type PartitionEpochItem struct {
	index       int
	epoch       int64
	partitionId int
}

// A PartitionEpochQueue implements heap.Interface and holds Items.
type PartitionEpochQueue []*PartitionEpochItem

func (pq PartitionEpochQueue) Len() int { return len(pq) }

func (pq PartitionEpochQueue) Less(i, j int) bool {
	// order by epoch in asc order
	return pq[i].epoch < pq[j].epoch
}

func (pq PartitionEpochQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PartitionEpochQueue) Push(x any) {
	n := len(*pq)
	item := x.(*PartitionEpochItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PartitionEpochQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
