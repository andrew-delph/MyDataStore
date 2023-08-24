package main

import (
	"container/heap"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/datap"
)

var (
	partitionEpochSynced = make([]int64, partitionBuckets)
	validFSM             = false
)
var partitionEpochQueue *PartitionEpochQueue

var checkQueueTick = make(chan struct{}, 1)

func managerInit() {
	partitionEpochQueue = &PartitionEpochQueue{}
	heap.Init(partitionEpochQueue)
	logrus.Warn("managerInit")

	for {
		if len(events.nodes) > N {
			logrus.Warnf("Manger Ready.")
			break
		}
		logrus.Warnf("Manger not Ready. Sleeping...")
		time.Sleep(time.Second * 5)
	}

	checkQueueTick <- struct{}{}

	run := true
	go func() {
		for run {
			select {
			case validFSMUpdate := <-validFSMObserver:
				if validFSMUpdate {
					if !validFSM {
						logrus.Warn("FSM is now valid.")
					}
					validFSM = true

				} else {
					if validFSM {
						logrus.Error("validFSM became false")
					}
					validFSM = false
				}
			// case <-time.After(time.Now().Add(time.Second).Sub(time.Now())):
			// 	logrus.Warn("Tick!")
			// 	checkQueueTick <- struct{}{}
			case <-checkQueueTick:
				if partitionEpochQueue.Len() > 0 {
					handlePartitionEpochItem()

					// sleep for the next item.
					item := partitionEpochQueue.PeekItem()
					if item != nil {
						go func() {
							select {
							case <-time.After(item.nextAttempt.Sub(time.Now())):
								checkQueueTick <- struct{}{}
							}
						}()
					}

				}
			case currEpoch = <-epochObserver:
				err := handleEpochUpdate(currEpoch)
				if err != nil {
					logrus.Errorf("handleEpochUpdate err = %v", err)
				}
			}
		}
	}()
}

// update the global merkletree bucket
// verify merkle trees are in sync with R nodes. (recent cache and global)
// if out of sync. it will call appropiate sync functions
func handleEpochUpdate(currEpoch int64) error {
	defer func() { checkQueueTick <- struct{}{} }()
	logrus.Debugf("currEpoch %d", currEpoch)
	if currEpoch-2 < 0 {
		return nil
	}

	myPartions, err := GetMemberPartions(events.consistent, conf.Name)
	if err != nil {
		logrus.Error(err)
		return err
	}
	lag := math.MaxInt32
	defer func() {
		if int(currEpoch)-lag > 3 {
			logrus.Warnf("currently lagging. diff = %d currEpoch = %d lag = %d", int(currEpoch)-lag, currEpoch, lag)
		}
	}()
	for _, partId := range myPartions {

		partition, err := store.getPartition(partId)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem getPartition err = %v", err)
			return err
		}
		tree, buckets, err := RawPartitionMerkleTree(currEpoch-2, false, partId)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem RawPartitionMerkleTree err = %v", err)
			return err
		}
		paritionEpochObject, err := MerkleTreeToParitionEpochObject(tree, buckets, currEpoch-2, partId)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem MerkleTreeToParitionEpochObject err = %v", err)
			return err
		}

		paritionEpochObject.Valid = false
		partition.PutParitionEpochObject(paritionEpochObject)
		lastValidParitionEpochObject, err := partition.LastParitionEpochObject()
		if err != nil && err != STORE_NOT_FOUND {
			logrus.Errorf("handlePartitionEpochItem LastParitionEpochObject err = %v", err)
			return err
		}

		lowerEpoch := int64(0)
		if lastValidParitionEpochObject != nil {
			lowerEpoch = lastValidParitionEpochObject.Epoch
		}
		lag = min(lag, int(lowerEpoch))
		partitionEpochQueue.PushItem(&PartitionEpochItem{
			epoch:       currEpoch - 2,
			partitionId: int(partId),
			nextAttempt: time.Now().Add(5 * time.Second),
		})

	}
	return nil
}

func handlePartitionEpochItem() {
	item := partitionEpochQueue.NextItem()
	// logrus.Warnf("handling item partion = %d epoch = %d", item.partitionId, item.epoch)
	// defer logrus.Warnf("DONE handling item partion = %d epoch = %d", item.partitionId, item.epoch)

	if item == nil {
		return
	}
	defer func() {
		if item.attempts > 2 && !item.completed {
			logrus.Warnf("Attempts Warning: e = %d p =  %d currEpoch = %d attemps = %d", item.epoch, item.partitionId, currEpoch, item.attempts)
		}
	}()

	item.attempts++
	item.nextAttempt = time.Now().Add(5 * time.Second)
	partitionEpochQueue.Fix(item)

	partition, err := store.getPartition(item.partitionId)
	if err != nil {
		logrus.Errorf("handlePartitionEpochItem err = %v", err)
		return
	}
	paritionEpochObject, err := partition.GetParitionEpochObject(int(item.epoch))
	if paritionEpochObject != nil && paritionEpochObject.Valid {
		item.completed = true
		return
	}
	if paritionEpochObject == nil {
		logrus.Warnf("paritionEpochObject is nil. err = %v", err)
		return
	}

	unsyncedBuckets, err := verifyPartitionEpochTree(paritionEpochObject)
	if err != nil {
		logrus.Debugf("verifyPartitionEpochTree partion = %d epoch = %d err = %v", item.partitionId, item.epoch, err)
	}
	if unsyncedBuckets != nil && len(unsyncedBuckets) > 0 {
		logrus.Debugf("confirmed unsynced nodes partion = %d epoch = %d # = %d", item.partitionId, item.epoch, len(unsyncedBuckets))

		// NOTE: possibly cool down node if streamed recently
		unsyncedNode := unsyncedBuckets[0]
		err = StreamBuckets(unsyncedNode.Addr, unsyncedNode.Diff, item.epoch, item.epoch+1, item.partitionId)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem RawPartitionMerkleTree err = %v", err)
			return
		}
		// TODO be polite...
		// var requestBuckets []int32
		// for _, buckedId := range unsyncedBuckets {
		// 	requestBuckets = append(requestBuckets, buckedId)
		// }
		// syncPartition(item.partitionId, requestBuckets, item.epoch, item.epoch+1)

		tree, buckets, err := RawPartitionMerkleTree(item.epoch, false, item.partitionId)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem RawPartitionMerkleTree err = %v", err)
			return
		}
		paritionEpochObject, err := MerkleTreeToParitionEpochObject(tree, buckets, item.epoch, item.partitionId)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem MerkleTreeToParitionEpochObject err = %v", err)
			return
		}

		// NOTE: possibly use bucketsCheck in the future...
		bucketsCheck := compare2dBytes(*unsyncedNode.Buckets, paritionEpochObject.Buckets)
		logrus.Debugf("bucketsCheck is %v", bucketsCheck)

		paritionEpochObject.Valid = false
		partition.PutParitionEpochObject(paritionEpochObject)

	} else {
		partition, err := store.getPartition(item.partitionId)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem err = %v", err)
			return
		}

		paritionEpochObject.Valid = true
		err = partition.PutParitionEpochObject(paritionEpochObject)
		if err != nil {
			logrus.Errorf("handlePartitionEpochItem err = %v", err)
			return
		}

		item.completed = true

		logrus.Debugf("Success sync of epoch = %d", currEpoch-1)
	}
}

type UnsyncedBuckets struct {
	Addr    string
	Diff    *[]int32
	Buckets *[][]byte
}

func verifyPartitionEpochTree(paritionEpochObject *datap.ParitionEpochObject) ([]*UnsyncedBuckets, error) {
	tree, err := ParitionEpochObjectToMerkleTree(paritionEpochObject)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	nodes, err := GetClosestNForPartition(events.consistent, int(paritionEpochObject.Partition), N)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	unsyncedCh := make(chan *UnsyncedBuckets, len(nodes))
	validCh := make(chan bool, len(nodes))
	errorCh := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(addr string) {
			otherParitionEpochObject, err := GetParitionEpochObject(addr, paritionEpochObject.Epoch, int(paritionEpochObject.Partition))
			if err != nil {
				logrus.Debug(err)
				errorCh <- err
				return
			}
			otherTree, err := ParitionEpochObjectToMerkleTree(otherParitionEpochObject)
			if err != nil {
				logrus.Errorf("verifyPartitionEpochTree ParitionEpochObjectToMerkleTree %v", err)
				errorCh <- err
				return
			}
			bucketsDiff := DifferentMerkleTreeBuckets(tree, otherTree)
			if bucketsDiff == nil {
				errorCh <- fmt.Errorf("bucketsDiff is nil")
			} else if len(bucketsDiff) == 0 {
				validCh <- true
			} else {
				logrus.Debug("bucketsDiff ", bucketsDiff)
				unsyncedCh <- &UnsyncedBuckets{Addr: addr, Diff: &bucketsDiff, Buckets: &otherParitionEpochObject.Buckets}
			}
		}(node.String())
	}

	timeout := time.After(time.Second * 40)
	responseCount := 0
	var unsyncedBucketsList []*UnsyncedBuckets
	for i := 0; i < len(nodes); i++ {
		select {
		case temp := <-unsyncedCh:
			unsyncedBucketsList = append(unsyncedBucketsList, temp)
		case <-validCh:
			responseCount++
		case err := <-errorCh:
			// logrus.Errorf("errorCh: %v", err)
			_ = err // Handle error if necessary
		case <-timeout:
			return unsyncedBucketsList, fmt.Errorf("timed out waiting for responses")
		}
		if responseCount >= R {
			return nil, nil
		}
	}
	return unsyncedBucketsList, fmt.Errorf("not enough nodes verifyied.")
}

// Note: Its not very polite.
// func syncPartition(partitionId int, requestBuckets []int32, lowerEpoch, upperEpoch int64) error {
// 	nodes, err := GetClosestNForPartition(events.consistent, partitionId, N)
// 	if err != nil {
// 		logrus.Error(err)
// 		return err
// 	}
// 	logrus.Debugf("StreamBuckets partitionId = %d lowerEpoch = %d upperEpoch = %d bucketsnum = %d", partitionId, lowerEpoch, upperEpoch, len(requestBuckets))
// 	var wg sync.WaitGroup

// 	for i := 0; i < len(nodes); i++ {
// 		wg.Add(1)
// 		go func(addr string) {
// 			defer wg.Done()
// 			StreamBuckets(nodes[rand.Intn(len(nodes))].String(), requestBuckets, lowerEpoch, upperEpoch, partitionId)
// 		}(nodes[i].String())
// 	}

// 	wg.Wait()
// 	return nil
// }

// An PartitionEpochItem is something we manage in a priority queue.
type PartitionEpochItem struct {
	epoch       int64
	partitionId int
	index       int
	completed   bool
	attempts    int
	nextAttempt time.Time
}

// A PartitionEpochQueue implements heap.Interface and holds Items.
type PartitionEpochQueue []*PartitionEpochItem

func (pq PartitionEpochQueue) Len() int {
	return len(pq)
}

func (pq PartitionEpochQueue) Less(i, j int) bool {
	// order by epoch in asc order
	if pq[i].epoch == pq[j].epoch {
		return pq[i].nextAttempt.Before(pq[j].nextAttempt)
	}
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

var queueLock sync.RWMutex

func (pq *PartitionEpochQueue) PushItem(item *PartitionEpochItem) {
	queueLock.Lock()
	defer queueLock.Unlock()
	heap.Push(pq, item)
}

func (pq *PartitionEpochQueue) PopItem() *PartitionEpochItem {
	queueLock.Lock()
	defer queueLock.Unlock()
	if len(*pq) == 0 {
		return nil
	}
	popVal, ok := heap.Pop(pq).(*PartitionEpochItem)
	if !ok {
		logrus.Fatal("FAILED TO DECODE POP.")
	}
	return popVal
}

func (pq *PartitionEpochQueue) PeekItem() *PartitionEpochItem {
	queueLock.RLock()
	defer queueLock.RUnlock()

	if len(*pq) == 0 {
		return nil
	}
	return (*pq)[0]
}

func (pq *PartitionEpochQueue) NextItem() *PartitionEpochItem {
	queueLock.Lock()
	defer queueLock.Unlock()

	var item *PartitionEpochItem

	for len(*pq) > 0 {
		item = (*pq)[0]
		if item.completed {
			heap.Pop(pq)
		} else if time.Now().Before(item.nextAttempt) {
			return nil
		} else {
			return item
		}
	}
	return nil
}

func (pq *PartitionEpochQueue) Fix(item *PartitionEpochItem) *PartitionEpochItem {
	queueLock.Lock()
	defer queueLock.Unlock()

	if len(*pq) < item.index || (*pq)[item.index] != item {
		logrus.Fatalf("Fix index out of sync! %v %v", &(*pq)[item.index], &item)
	}

	heap.Fix(pq, item.index)

	return nil
}
