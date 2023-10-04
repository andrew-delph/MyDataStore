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
	wg             *sync.WaitGroup
}

type VerifyPartitionEpochEvent struct {
	Epoch int64
}

type ConsistencyController struct {
	heap             *ConsistencyHeap
	partitionCount   int
	observationCh    chan rxgo.Item
	partitionsStates []*PartitionState
	observable       rxgo.Observable
	currPartitions   utils.IntSet
	reqCh            chan interface{}
	working          int32
}

func NewConsistencyController(concurrencyLevel int, partitionCount int, reqCh chan interface{}) *ConsistencyController {
	heap := NewConsistencyHeap()
	// TODO create semaphore.NewWeighted(int64(limit)) for number of partition observer events at once
	ch := make(chan rxgo.Item)
	observable := rxgo.FromChannel(ch, rxgo.WithPublishStrategy())
	var partitionsStates []*PartitionState
	for i := 0; i < partitionCount; i++ {
		partitionState := NewPartitionState(i, observable, heap)
		partitionState.StartConsumer()
		partitionsStates = append(partitionsStates, partitionState)
	}
	observable.Connect(context.Background())
	cc := &ConsistencyController{
		heap:             heap,
		observationCh:    ch,
		partitionsStates: partitionsStates,
		observable:       observable,
		partitionCount:   partitionCount,
		reqCh:            reqCh,
	}
	for i := 0; i < concurrencyLevel; i++ {
		go cc.startWorker()
	}
	return cc
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
	var wg sync.WaitGroup
	wg.Add(cc.partitionCount)
	cc.PublishPartitions(currPartitions, &wg)
	wg.Wait()
	return nil
}

func (cc *ConsistencyController) startWorker() error {
	for {
		partitionQueueSize.Set(float64(cc.heap.Size()))
		item := cc.heap.PopItem()
		// logrus.Warn("ConsistencyController item=", item)
		// defer logrus.Warn("DONE ConsistencyController item=", item)
		atomic.AddInt32(&cc.working, 1)
		if item.SyncTask {
			cc.SyncPartition(item)
		} else {
			cc.VerifyPartitionEpoch(item)
		}
		atomic.AddInt32(&cc.working, -1)
	}
}

func (cc *ConsistencyController) IsPartitionActive(partitionId int) bool {
	if partitionId >= len(cc.partitionsStates) {
		return false
	}
	return cc.partitionsStates[partitionId].active.Load()
}

func (cc *ConsistencyController) PublishEpoch(Epoch int64) {
	logrus.Debugf("new Epoch %d", Epoch)
	cc.PublishEvent(VerifyPartitionEpochEvent{Epoch: Epoch})
}

func (cc *ConsistencyController) PublishPartitions(currPartitions utils.IntSet, wg *sync.WaitGroup) {
	logrus.Debugf("new Partitions %d", currPartitions)
	cc.PublishEvent(UpdatePartitionsEvent{CurrPartitions: currPartitions, wg: wg})
}

func (cc *ConsistencyController) PublishEvent(event interface{}) {
	cc.observationCh <- rxgo.Of(event)
}

func (cc *ConsistencyController) VerifyPartitionEpoch(item ConsistencyItem) {
	if cc.IsPartitionActive(item.PartitionId) == false {
		return
	}
	resCh := make(chan interface{})
	logrus.Debugf("Verify partition %d epoch %d", item.PartitionId, item.Epoch)
	cc.reqCh <- VerifyPartitionEpochRequestTask{PartitionId: item.PartitionId, Epoch: item.Epoch, ResCh: resCh}
	rawRes := <-resCh
	switch res := rawRes.(type) {
	case VerifyPartitionEpochResponse:
		// logrus.Warnf("VerifyPartitionEpoch E= %d res = %+v", item.Epoch, res)
	case error:
		err := errors.Wrap(res, "VerifyPartitionEpoch")
		logrus.Debug(err)
		logrus.Error(err)
		cc.heap.RequeueItem(item)

	default:
		logrus.Panicf(" response unkown res type: %v", reflect.TypeOf(res))
	}
}

func (cc *ConsistencyController) SyncPartition(item ConsistencyItem) {
	if cc.IsPartitionActive(item.PartitionId) == false {
		return
	}
	lower := int64(0)
	// }
	logrus.Warnf("SyncPartition p %d lower %d", item.PartitionId, lower)
	for i := lower; i < item.Epoch; i++ {
		logrus.Warnf("sync queue verify p %d e %d", item.PartitionId, i)
		cc.heap.PushVerifyTask(item.PartitionId, i)
	}
	logrus.Debugf("new partition sync %d", item.PartitionId)
	resCh := make(chan interface{})
	cc.reqCh <- SyncPartitionTask{PartitionId: int32(item.PartitionId), ResCh: resCh, UpperEpoch: item.Epoch}
	rawRes := <-resCh
	switch res := rawRes.(type) {
	case SyncPartitionResponse:
		if res.Valid {
			logrus.Debugf("SyncPartition:  sync partrition %d res = %+v", item.PartitionId, res)
		} else {
			logrus.Debugf("SyncPartition:  err partrition %d res = %+v", item.PartitionId, res)
		}
		// lower := res.LowerEpoch
		// // if lower < 0 {
		// lower = 0
		// // }
		// logrus.Warnf("SyncPartition p %d lower %d", item.PartitionId, lower)
		// for i := lower; i < res.UpperEpoch; i++ {
		// 	logrus.Warnf("sync queue verify p %d e %d", item.PartitionId, i)
		// 	cc.heap.PushVerifyTask(item.PartitionId, i)
		// }
	case nil:
		logrus.Debugf("SyncPartition: DOESNT NEED TO SYNC")
	case error:
		logrus.Error(errors.Wrap(res, "SyncPartition"))
		// cc.heap.RequeueItem(item)
	default:
		logrus.Panicf("SyncPartition: unkown res type: %v", reflect.TypeOf(res))
	}
}

func (cc *ConsistencyController) IsHealthy() error {
	heapLen := cc.heap.Len()
	working := atomic.LoadInt32(&cc.working)
	if working > 0 || heapLen > 0 {
		return fmt.Errorf("heapLen %d working %d", heapLen, working)
	}
	return nil
}

type PartitionState struct {
	partitionId  int
	heap         *ConsistencyHeap
	active       atomic.Bool
	lastEpoch    int64
	observable   rxgo.Observable
	activeEpochs *utils.IntSet
}

func NewPartitionState(partitionId int, observable rxgo.Observable, heap *ConsistencyHeap) *PartitionState {
	activeEpochs := utils.NewIntSet()
	ps := &PartitionState{partitionId: partitionId, observable: observable, activeEpochs: &activeEpochs, heap: heap}
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
			lower := int64(0)
			if ps.active.Load() {
				for i := lower; i < ps.lastEpoch; i++ {
					logrus.Warnf("sync queue verify p %d e %d", ps.partitionId, i)
					ps.heap.PushVerifyTask(ps.partitionId, i)
				}
				// ps.heap.PushVerifyTask(ps.partitionId, ps.lastEpoch)
			}

		case UpdatePartitionsEvent: // TODO create test case for this
			defer event.wg.Done()
			partitionLabel := fmt.Sprintf("%d", ps.partitionId)
			logrus.Debug("partitionLabel = ", partitionLabel)
			if ps.active.Load() {
				partitionActive.WithLabelValues(partitionLabel).Set(1)
			} else {
				partitionActive.WithLabelValues(partitionLabel).Set(0)
			}
			if event.CurrPartitions.Has(ps.partitionId) && ps.active.CompareAndSwap(false, true) { // TODO create test case for this
				ps.heap.PushSyncTask(ps.partitionId, ps.lastEpoch)
			} else if !event.CurrPartitions.Has(ps.partitionId) && ps.active.CompareAndSwap(true, false) {
				logrus.Debugf("updated lost partition active %d", ps.partitionId)
			}

			if event.CurrPartitions.Has(ps.partitionId) != ps.active.Load() {
				logrus.Fatal("active partition did not switch") // remove this once unit tested
			}
		default:
			logrus.Warnf("unknown PartitionState event : %v", reflect.TypeOf(event))
		}
	})
	return nil
}
