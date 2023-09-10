package main

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/reactivex/rxgo/v2"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/utils"
)

type PartitionLocker struct {
	partitionLockMap sync.Map
}

func NewPartitionLocker(partitionCount int) *PartitionLocker {
	pl := &PartitionLocker{}
	for i := 0; i < partitionCount; i++ {
		pl.partitionLockMap.Store(i, false)
	}
	return pl
}

func (pl *PartitionLocker) Lock(partition int) error {
	swapped := pl.partitionLockMap.CompareAndSwap(partition, false, true)
	if !swapped {
		return errors.New("could not swap key")
	}
	return nil
}

func (pl *PartitionLocker) Unlock(partition int) error {
	pl.partitionLockMap.Store(partition, false)
	return nil
}

func (m *Manager) HandleHashringChange() error {
	// logrus.Warn("HandleHashringChange!!!!!!!!!!")
	return nil
	m.consistencyController.PublishEvent("CHANGED PARTITIONS")
	currPartitionsList, err := m.ring.GetMyPartions()
	if err != nil {
		return err
	}
	m.consistencyController.PublishEvent("CHANGED PARTITIONS")
	currPartitions := utils.NewIntSet().From(currPartitionsList)
	new := currPartitions.Difference(*m.myPartitions)
	lost := m.myPartitions.Difference(currPartitions)
	if len(new) > 0 || len(lost) > 0 {
		logrus.Warnf("new %d lost %d", len(new), len(lost))
	}
	m.myPartitions = &currPartitions

	for toSync := range new {
		err := m.SyncPartition(toSync)
		if err != nil {
			logrus.Error(err)
		}

	}
	return nil
}

func (m *Manager) VerifyEpoch(Epoch int64) {
	// logrus.Warnf("verifying epoch %d", Epoch)
}

func (m *Manager) LastValidEpoch(partition int) int {
	logrus.Debugf("LastValidEpoch for partition %d", partition)
	return 0
}

func (m *Manager) SyncPartition(partition int) error {
	// logrus.Warnf("SyncPartition %d", partition)
	err := m.partitionLocker.Lock(partition)
	if err != nil {
		return err
	}
	defer m.partitionLocker.Unlock(partition)

	lastValidEpoch := m.LastValidEpoch(partition)
	logrus.Debugf("lastValidEpoch for partion %d is %d", partition, lastValidEpoch)

	// should sync all values from lastValidEpoch + to including current epoch

	// stream values from a node that has the highest health epoch for the partitions.
	return nil
}

type ConsistencyController struct {
	observationCh    chan rxgo.Item
	partitionsStates []*PartitionState
	observable       rxgo.Observable
}

func NewConsistencyController(partitionCount int) *ConsistencyController {
	ch := make(chan rxgo.Item)
	observable := rxgo.FromChannel(ch, rxgo.WithPublishStrategy())
	var partitionsStates []*PartitionState
	for i := 0; i < partitionCount; i++ {
		partitionsStates = append(partitionsStates, NewPartitionState(i, observable))
	}
	observable.Connect(context.Background())
	return &ConsistencyController{observationCh: ch, partitionsStates: partitionsStates, observable: observable}
}

func (cc *ConsistencyController) PublishEvent(event interface{}) {
	cc.observationCh <- rxgo.Of(event)
}

type PartitionState struct {
	updating    bool
	partitionId int
}

func NewPartitionState(partitionId int, observable rxgo.Observable) *PartitionState {
	ps := &PartitionState{partitionId: partitionId}
	observable.DoOnNext(func(item interface{}) {
		logrus.Warnf("PartitionState: pId=%d: item=%v", partitionId, item)
	})
	return ps
}

// called on new epoch or aquired new partitions
// for now assume the partition can be partial in the past
func (ps *PartitionState) Balance() {
	// if lagging start sync function
}

func (ps *PartitionState) Verify() {
	// if lagging start sync function
}
