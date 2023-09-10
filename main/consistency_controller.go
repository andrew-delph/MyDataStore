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

type PartitionsUpdateEvent struct {
	CurrPartitions utils.IntSet
}

type PartitionEpochVerifyEvent struct {
	Epoch int64
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

func (cc *ConsistencyController) HandleHashringChange(currPartitions utils.IntSet) error {
	cc.PublishEvent(PartitionsUpdateEvent{CurrPartitions: currPartitions})
	return nil
}

func (cc *ConsistencyController) VerifyEpoch(Epoch int64) {
	cc.PublishEvent(PartitionEpochVerifyEvent{Epoch: Epoch})
}

func (cc *ConsistencyController) PublishEvent(event interface{}) {
	cc.observationCh <- rxgo.Of(event)
}

type PartitionState struct {
	updating    bool
	partitionId int
	active      bool
}

func NewPartitionState(partitionId int, observable rxgo.Observable) *PartitionState {
	ps := &PartitionState{partitionId: partitionId}
	observable.DoOnNext(func(item interface{}) {
		switch event := item.(type) {
		case PartitionEpochVerifyEvent:
			logrus.Warnf("PartitionEpochVerifyEvent partition %d epoch %d", partitionId, event.Epoch)

		case PartitionsUpdateEvent:
			if !ps.active && event.CurrPartitions.Has(partitionId) {
				logrus.Warnf("new partition %d", partitionId)
			} else if ps.active && !event.CurrPartitions.Has(partitionId) {
				logrus.Warnf("lost partition %d", partitionId)
			}
			ps.active = event.CurrPartitions.Has(partitionId)
		default:
			logrus.Warn("unknown PartitionState event")
		}
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
