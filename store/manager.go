package main

import (
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	partitionEpochSynced = make([]int64, partitionBuckets)
	validFSM             = false
)

func managerInit() {
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
			}
		}
	}()
}

// verify that the cached partition global merkletree is in sync with R nodes
func verifyPartitionGlobal(partitionId int) (*map[int32]struct{}, error) {
	return verifyPartition(partitionId, currGlobalBucketEpoch, true)
}

// verify that the cached partition epoch merkletree is in sync with R nodes
func verifyPartitionEpoch(partitionId int, epoch int64) (*map[int32]struct{}, error) {
	return verifyPartition(partitionId, epoch, false)
}

func verifyPartition(partitionId int, epoch int64, global bool) (*map[int32]struct{}, error) {
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
			unsyncedBuckets, err = verifyPartitionEpoch(partId, currEpoch-1)
		} else {
			logrus.Warnf("checking global")
			unsyncedBuckets, err = verifyPartitionGlobal(partId)
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
