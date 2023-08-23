package main

import (
	"container/heap"
	"fmt"
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

	for {
		if len(events.nodes) > N {
			logrus.Warnf("Manger Ready.")
			break
		}
		logrus.Warnf("Manger not Ready. Sleeping...")
		time.Sleep(time.Second * 5)
	}

	run := true
	go func() {
		for run {
			select {
			case validFSMUpdate := <-validFSMObserver:
				if validFSMUpdate {
					if !validFSM {
						logrus.Warn("FSM is now valid.")
						err := QueueMyPartitionEpochItems()
						if err != nil {
							logrus.Fatal(err)
						}
					}
					validFSM = true

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
	// for partId := 0; partId > partitionCount; partId++ {
	// 	tree, buckets, err := RawPartitionMerkleTree(currEpoch-2, false, partId)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	partition, err := store.getPartition(partId)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	paritionEpochObject, err := MerkleTreeToParitionEpochObject(tree, buckets, currEpoch-2, partId)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	err = partition.PutParitionEpochObject(paritionEpochObject)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func QueueMyPartitionEpochItems() error {
	myPartions, err := GetMemberPartions(events.consistent, conf.Name)
	if err != nil {
		logrus.Error(err)
		return err
	}
	for _, partId := range myPartions {
		partition, err := store.getPartition(partId)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem err = %v", err)
			return err
		}
		paritionEpochObject, err := partition.LastParitionEpochObject()
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem err = %v", err)
			return err
		}
		if paritionEpochObject == nil {
			partitionEpochQueue.Push(&PartitionEpochItem{
				epoch:       0,
				partitionId: int(partition.GetPartitionId()),
			})
		} else {
			partitionEpochQueue.Push(&PartitionEpochItem{
				epoch:       paritionEpochObject.Epoch + 1,
				partitionId: int(paritionEpochObject.Partition),
			})
		}

	}
	return nil
}

func handlePartitionEpochItem() {
	item := heap.Pop(&partitionEpochQueue).(*PartitionEpochItem)
	// logrus.Warnf("handling item partion = %d epoch = %d", item.partitionId, item.epoch)
	// defer logrus.Warnf("DONE handling item partion = %d epoch = %d", item.partitionId, item.epoch)

	delayFailedItem := func() {
		go func() {
			time.Sleep(10 * time.Second)
			partitionEpochQueue.Push(item)
		}()
	}
	if item.epoch > currEpoch-2 {
		delayFailedItem()
		return
	}

	tree, bucketList, err := RawPartitionMerkleTree(item.epoch, false, item.partitionId)
	if err != nil {
		logrus.Errorf("handlePartitionEpochItem err = %v", err)
		delayFailedItem()
		return
	}
	unsyncedBuckets, err := verifyPartitionEpochTree(tree, item.partitionId, item.epoch)
	if (unsyncedBuckets != nil && len(unsyncedBuckets) > 0) || err != nil {
		delayFailedItem()
		logrus.Errorf("failed to sync partion = %d epoch = %d err = %v", item.partitionId, item.epoch, err)
		var requestBuckets []int32
		for _, buckedId := range unsyncedBuckets {
			requestBuckets = append(requestBuckets, buckedId)
		}
		syncPartition(item.partitionId, requestBuckets, item.epoch, item.epoch+1)

	} else {
		partition, err := store.getPartition(item.partitionId)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem err = %v", err)
			delayFailedItem()
			return
		}

		paritionEpochObject, err := MerkleTreeToParitionEpochObject(tree, bucketList, item.epoch, item.partitionId)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem err = %v", err)
			delayFailedItem()
			return
		}
		err = partition.PutParitionEpochObject(paritionEpochObject)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem err = %v", err)
			delayFailedItem()
			return
		}

		logrus.Debugf("Success sync of epoch = %d", currEpoch-1)
		partitionEpochQueue.Push(&PartitionEpochItem{
			epoch:       item.epoch + 1,
			partitionId: item.partitionId,
		})
	}
}

func verifyPartitionEpochTree(tree *merkletree.MerkleTree, partitionId int, epoch int64) ([]int32, error) {
	nodes, err := GetClosestNForPartition(events.consistent, int(partitionId), N)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	unsyncedCh := make(chan []int32, len(nodes))
	validCh := make(chan bool, len(nodes))
	errorCh := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(addr string) {
			otherParitionEpochObject, err := GetParitionEpochObject(addr, epoch, partitionId)
			if err != nil {
				logrus.Error(err)
				errorCh <- err
				return
			}
			otherTree, err := ParitionEpochObjectToMerkleTree(otherParitionEpochObject)
			if err != nil {
				logrus.Error(err)
				errorCh <- err
				return
			}
			unsyncedBuckets := DifferentMerkleTreeBuckets(tree, otherTree)
			if unsyncedBuckets != nil && len(unsyncedBuckets) == 0 {
				validCh <- true
			} else if unsyncedBuckets == nil {
				errorCh <- fmt.Errorf("unsyncedBuckets is nil")
			} else {
				logrus.Warn("unsyncedBuckets ", unsyncedBuckets)
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
			unsyncedBuckets = temp
		case <-validCh:
			responseCount++
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
}

func (pq *PartitionEpochQueue) Push(x any) {
	item := x.(*PartitionEpochItem)
	*pq = append(*pq, item)
}

func (pq *PartitionEpochQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}
