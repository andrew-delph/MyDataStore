package main

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/reactivex/rxgo/v2"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

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

type VerifyPartitionEpochRequestTask struct {
	PartitionId int
	Epoch       int64
	ResCh       chan interface{}
}

type VerifyPartitionEpochResponse struct {
	Valid bool
}

type SyncPartitionTask struct {
	PartitionId int32
	UpperEpoch  int64
	ResCh       chan interface{}
}

type SyncPartitionResponse struct {
	Valid bool
}

type UpdatePartitionsEvent struct {
	CurrPartitions utils.IntSet
}

type VerifyPartitionEpochEvent struct {
	Epoch int64
}

type ConsistencyController struct {
	observationCh    chan rxgo.Item
	partitionsStates []*PartitionState
	observable       rxgo.Observable
}

func NewConsistencyController(concurrencyLevel int64, partitionCount int, reqCh chan interface{}) *ConsistencyController {
	sema := semaphore.NewWeighted(concurrencyLevel)
	// TODO create semaphore.NewWeighted(int64(limit)) for number of partition observer events at once
	ch := make(chan rxgo.Item)
	observable := rxgo.FromChannel(ch, rxgo.WithPublishStrategy())
	var partitionsStates []*PartitionState
	for i := 0; i < partitionCount; i++ {
		partitionsStates = append(partitionsStates, NewPartitionState(sema, i, observable, reqCh))
	}
	observable.Connect(context.Background())
	return &ConsistencyController{observationCh: ch, partitionsStates: partitionsStates, observable: observable}
}

func (cc *ConsistencyController) HandleHashringChange(currPartitions utils.IntSet) error {
	cc.PublishEvent(UpdatePartitionsEvent{CurrPartitions: currPartitions})
	return nil
}

func (cc *ConsistencyController) VerifyEpoch(Epoch int64) {
	logrus.Warnf("new Epoch %d", Epoch)
	cc.PublishEvent(VerifyPartitionEpochEvent{Epoch: Epoch})
}

func (cc *ConsistencyController) PublishEvent(event interface{}) {
	cc.observationCh <- rxgo.Of(event)
}

type PartitionState struct {
	partitionId int
	active      atomic.Bool
	lastEpoch   int64
}

func NewPartitionState(sema *semaphore.Weighted, partitionId int, observable rxgo.Observable, reqCh chan interface{}) *PartitionState {
	ps := &PartitionState{partitionId: partitionId}
	observable.DoOnNext(func(item interface{}) {
		err := sema.Acquire(context.Background(), 1)
		if err != nil {
			logrus.Error(err)
		} else {
			defer sema.Release(1)
		}
		switch event := item.(type) {
		case VerifyPartitionEpochEvent: // TODO create test case for this
			ps.lastEpoch = event.Epoch - 2
			if ps.lastEpoch < 0 {
				return
			}
			if ps.active.Load() {
				resCh := make(chan interface{})
				logrus.Debugf("Verify partition %d epoch %d", partitionId, ps.lastEpoch)
				reqCh <- VerifyPartitionEpochRequestTask{PartitionId: partitionId, Epoch: ps.lastEpoch, ResCh: resCh}
				rawRes := <-resCh
				switch res := rawRes.(type) {
				case VerifyPartitionEpochResponse:
					logrus.Debugf("VerifyPartitionEpochResponse E= %d res = %+v", ps.lastEpoch, res)
				case error:
					err := errors.Wrap(res, "VerifyPartitionEpochEvent response")
					logrus.Error(err)
				default:
					logrus.Panicf("VerifyPartitionEpochEvent observer unkown res type: %v", reflect.TypeOf(res))
				}
			}

		case UpdatePartitionsEvent: // TODO create test case for this
			if event.CurrPartitions.Has(partitionId) && ps.active.CompareAndSwap(false, true) { // TODO create test case for this

				logrus.Warnf("new partition sync %d", partitionId)
				resCh := make(chan interface{})
				reqCh <- SyncPartitionTask{PartitionId: int32(partitionId), ResCh: resCh, UpperEpoch: ps.lastEpoch}
				rawRes := <-resCh
				switch res := rawRes.(type) {
				case SyncPartitionResponse:
					logrus.Debugf("sync partrition %d res = %+v", partitionId, res)
				case error:
					logrus.Error(errors.Wrap(res, "UpdatePartitionsEvent res"))
				default:
					logrus.Panicf("UpdatePartitionsEvent observer unkown res type: %v", reflect.TypeOf(res))
				}
			} else if !event.CurrPartitions.Has(partitionId) && ps.active.CompareAndSwap(true, false) {
				logrus.Warnf("updated lost partition active %d", partitionId)
			}

			partitionLabel := fmt.Sprintf("%d", partitionId)
			logrus.Debug("partitionLabel = ", partitionLabel)
			if ps.active.Load() {
				partitionActiveGague.WithLabelValues(partitionLabel).Set(1)
			} else {
				partitionActiveGague.WithLabelValues(partitionLabel).Set(0)
			}

			if event.CurrPartitions.Has(partitionId) != ps.active.Load() {
				logrus.Fatal("active partition did not switch") // remove this once unit tested
			}
		default:
			logrus.Warn("unknown PartitionState event : %v", reflect.TypeOf(event))
		}
	})
	return ps
}
