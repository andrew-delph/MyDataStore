package main

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

var (
	partitionEpochSynced = make(chan []int, partitionBuckets)
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
					logrus.Warn("FSM is now valid.")
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

func syncPartition(partitionId int) {
	// must be in sync with R of partitions to increment.
	logrus.Warnf("syncPartition %d", partitionId)
}

func verifyPartition(partitionId int) {
	// must be in sync with R of partitions to increment.
	logrus.Warnf("verifyPartition %d", partitionId)
}

func handleEpochUpdate(currEpoch int64) error {
	// must be in sync with R of partitions to increment.
	logrus.Warnf("currEpoch %d", currEpoch)
	err := UpdateGlobalBucket(currEpoch)
	if err != nil {
		logrus.Error(fmt.Errorf("UpdateGlobalBucket err = %v", err))
		return err
	}

	if validFSM {
		err = RecentEpochSync()
		if err != nil {
			logrus.Error(fmt.Errorf("RecentEpochSync err = %v", err))
			return err
		}
	}
	return nil
}
