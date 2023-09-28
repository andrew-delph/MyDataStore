package main

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/reactivex/rxgo/v2"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	"github.com/andrew-delph/my-key-store/utils"
)

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
	Valid      bool
	LowerEpoch int64
	UpperEpoch int64
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
	sema             *semaphore.Weighted
	concurrencyLevel int64
	currPartitions   utils.IntSet
}

func NewConsistencyController(concurrencyLevel int64, partitionCount int, reqCh chan interface{}) *ConsistencyController {
	sema := semaphore.NewWeighted(concurrencyLevel)
	// TODO create semaphore.NewWeighted(int64(limit)) for number of partition observer events at once
	ch := make(chan rxgo.Item)
	observable := rxgo.FromChannel(ch, rxgo.WithPublishStrategy())
	var partitionsStates []*PartitionState
	for i := 0; i < partitionCount; i++ {
		partitionState := NewPartitionState(sema, i, observable, reqCh)
		partitionState.StartConsumer()
		partitionsStates = append(partitionsStates, partitionState)
	}
	observable.Connect(context.Background())
	return &ConsistencyController{observationCh: ch, partitionsStates: partitionsStates, observable: observable, sema: sema, concurrencyLevel: concurrencyLevel}
}

func (cc *ConsistencyController) HandleHashringChange(currPartitions utils.IntSet) error {
	gained := len(currPartitions.Difference(cc.currPartitions))
	lost := len(cc.currPartitions.Difference(currPartitions))
	partitionsLost.Add(float64(lost))
	partitionsGained.Add(float64(gained))
	partitionsTotal.Set(float64(len(currPartitions.List())))
	if lost > 0 || gained > 0 {
		logrus.Warnf("partitions lost  %d gained %d", lost, gained)
	}
	cc.currPartitions = currPartitions
	cc.PublishEvent(UpdatePartitionsEvent{CurrPartitions: currPartitions})
	return nil
}

func (cc *ConsistencyController) VerifyEpoch(Epoch int64) {
	logrus.Debugf("new Epoch %d", Epoch)
	cc.PublishEvent(VerifyPartitionEpochEvent{Epoch: Epoch})
}

func (cc *ConsistencyController) PublishEvent(event interface{}) {
	cc.observationCh <- rxgo.Of(event)
}

func (cc *ConsistencyController) IsBusy() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := cc.sema.Acquire(ctx, cc.concurrencyLevel)
	if err == nil {
		cc.sema.Release(cc.concurrencyLevel)
	}
	return nil
}

func (cc *ConsistencyController) IsHealthy() error {
	for _, partitionState := range cc.partitionsStates {
		epochs := partitionState.GetActivateEpochs()
		if epochs > 0 {
			return errors.Errorf("partitionState epochs %d", epochs)
		}
	}
	return nil
}

type PartitionState struct {
	partitionId  int
	active       atomic.Bool
	lastEpoch    int64
	sema         *semaphore.Weighted
	observable   rxgo.Observable
	reqCh        chan interface{}
	activeEpochs *utils.IntSet
	activeLock   sync.Mutex
}

func NewPartitionState(sema *semaphore.Weighted, partitionId int, observable rxgo.Observable, reqCh chan interface{}) *PartitionState {
	activeEpochs := utils.NewIntSet()
	ps := &PartitionState{partitionId: partitionId, observable: observable, sema: sema, reqCh: reqCh, activeEpochs: &activeEpochs}
	return ps
}

func (ps *PartitionState) StartConsumer() error {
	ps.observable.DoOnNext(func(item interface{}) {
		switch event := item.(type) {
		case VerifyPartitionEpochEvent: // TODO create test case for this
			ps.lastEpoch = event.Epoch - 2
			if ps.lastEpoch < 0 {
				return
			}
			if ps.active.Load() {
				go ps.VerifyPartitionEpoch(ps.lastEpoch)
			}

		case UpdatePartitionsEvent: // TODO create test case for this
			partitionLabel := fmt.Sprintf("%d", ps.partitionId)
			logrus.Debug("partitionLabel = ", partitionLabel)
			if ps.active.Load() {
				partitionActive.WithLabelValues(partitionLabel).Set(1)
			} else {
				partitionActive.WithLabelValues(partitionLabel).Set(0)
			}

			if event.CurrPartitions.Has(ps.partitionId) && ps.active.CompareAndSwap(false, true) { // TODO create test case for this
				go ps.SyncPartition(ps.lastEpoch)
			} else if !event.CurrPartitions.Has(ps.partitionId) && ps.active.CompareAndSwap(true, false) {
				logrus.Debugf("updated lost partition active %d", ps.partitionId)
			}

			if event.CurrPartitions.Has(ps.partitionId) != ps.active.Load() {
				logrus.Fatal("active partition did not switch") // remove this once unit tested
			}
		default:
			logrus.Warn("unknown PartitionState event : %v", reflect.TypeOf(event))
		}
	})
	return nil
}

func (ps *PartitionState) VerifyPartitionEpoch(Epoch int64) {
	if ps.active.Load() == false {
		ps.DeactivateEpoch(Epoch)
		return
	}
	ps.ActivateEpoch(Epoch)
	err := ps.sema.Acquire(context.Background(), 1)
	if err != nil {
		logrus.Fatal(err)
	} else {
		defer ps.sema.Release(1)
	}
	resCh := make(chan interface{})
	logrus.Debugf("Verify partition %d epoch %d", ps.partitionId, Epoch)
	ps.reqCh <- VerifyPartitionEpochRequestTask{PartitionId: ps.partitionId, Epoch: Epoch, ResCh: resCh}
	rawRes := <-resCh
	switch res := rawRes.(type) {
	case VerifyPartitionEpochResponse:
		ps.DeactivateEpoch(Epoch)
		logrus.Warnf("VerifyPartitionEpoch E= %d res = %+v", Epoch, res)
	case error:
		err := errors.Wrap(res, "VerifyPartitionEpoch")
		logrus.Error(err)
		go ps.VerifyPartitionEpoch(Epoch)
	default:
		logrus.Panicf(" response unkown res type: %v", reflect.TypeOf(res))
	}
}

func (ps *PartitionState) SyncPartition(UpperEpoch int64) {
	err := ps.sema.Acquire(context.Background(), 1)
	if err != nil {
		logrus.Fatal(err)
	} else {
		defer ps.sema.Release(1)
	}
	logrus.Debugf("new partition sync %d", ps.partitionId)
	resCh := make(chan interface{})
	ps.reqCh <- SyncPartitionTask{PartitionId: int32(ps.partitionId), ResCh: resCh, UpperEpoch: UpperEpoch}
	rawRes := <-resCh
	switch res := rawRes.(type) {
	case SyncPartitionResponse:
		if res.Valid {
			logrus.Debugf("SyncPartition:  sync partrition %d res = %+v", ps.partitionId, res)
		} else {
			logrus.Debugf("SyncPartition:  err partrition %d res = %+v", ps.partitionId, res)
		}
		for i := res.LowerEpoch; i < res.UpperEpoch; i++ {
			go ps.VerifyPartitionEpoch(i)
		}
	case nil:
		logrus.Debugf("SyncPartition: DOESNT NEED TO SYNC")
	case error:
		logrus.Debug(errors.Wrap(res, "SyncPartition"))
		time.Sleep(time.Second * 10)
		go ps.SyncPartition(UpperEpoch)
	default:
		logrus.Panicf("SyncPartition: unkown res type: %v", reflect.TypeOf(res))
	}
}

func (ps *PartitionState) ActivateEpoch(Epoch int64) {
	ps.activeLock.Lock()
	defer ps.activeLock.Unlock()

	ps.activeEpochs.Add(int(Epoch))
	// logrus.Warnf("ActivateEpoch %d size %d", Epoch, len(ps.activeEpochs.List()))
}

func (ps *PartitionState) DeactivateEpoch(Epoch int64) {
	ps.activeLock.Lock()
	defer ps.activeLock.Unlock()
	ps.activeEpochs.Remove(int(Epoch))
	logrus.Warnf("DeactivateEpoch %d size %d", Epoch, len(ps.activeEpochs.List()))
}

func (ps *PartitionState) GetActivateEpochs() int {
	ps.activeLock.Lock()
	defer ps.activeLock.Unlock()

	return len(ps.activeEpochs.List())
}
