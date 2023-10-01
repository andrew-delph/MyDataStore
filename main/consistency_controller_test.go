package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/andrew-delph/my-key-store/utils"

	"github.com/reactivex/rxgo/v2"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	"github.com/stretchr/testify/assert"
)

func TestRxGoObservers(t *testing.T) {
	// t.Error("show")
	// use rxgo as works. use a semaphore to limit the number of active workers.
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	ch := make(chan rxgo.Item)
	// Create a Connectable Observable
	observable := rxgo.FromChannel(ch, rxgo.WithPublishStrategy())

	workers := 1
	items := 10
	limit := 2
	sem := semaphore.NewWeighted(int64(limit))
	var wg sync.WaitGroup
	wg.Add(workers * items)
	for i := 0; i < workers; i++ {
		workerId := i
		observable.DoOnNext(func(item interface{}) {
			sem.Acquire(context.Background(), 1)
			defer sem.Release(1)
			defer wg.Done()
			logrus.Warnf("w%d: %v", workerId, item)
			time.Sleep(time.Second)
		})
	}

	observable.Connect(context.Background())
	for i := 0; i < items; i++ {
		ch <- rxgo.Of(fmt.Sprintf("item%d", i))
	}

	logrus.Info("done")

	doneCh := make(chan struct{})

	// Start a goroutine to wait for wg to complete.
	go func() {
		wg.Wait()
		close(doneCh) // Signal completion to the channel when wg is done.
	}()

	select {
	case <-doneCh:
		return
	case <-time.After(time.Second * time.Duration((workers*items/limit)+1)): // Adjust the timeout duration as needed.
		t.Error("TIMEOUT")
	}
}

func TestConsistencyControllerObservers(t *testing.T) {
	// TODO show this
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	logrus.Info("HI")
	consistencyController := NewConsistencyController(2, 10, nil) // TODO impllement proper test

	consistencyController.PublishEvent("test")
	time.Sleep(time.Second * 5)
}

func TestConsistencyControllerActiveEpochs(t *testing.T) {
	// TODO show this
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	initMetrics("test")
	consistencyController := NewConsistencyController(2, 10, nil) // TODO impllement proper test
	err := consistencyController.IsHealthy()
	if err != nil {
		t.Error(err)
	}

	currPartitions := utils.NewIntSet()
	currPartitions.Add(1)
	currPartitions.Add(2)
	currPartitions.Add(3)
	consistencyController.HandleHashringChange(currPartitions)
	time.Sleep(time.Second)

	assert.Equal(t, true, consistencyController.IsPartitionActive(1), "consistencyController.IsPartitionActive")
	assert.Equal(t, true, consistencyController.IsPartitionActive(2), "consistencyController.IsPartitionActive")
	assert.Equal(t, true, consistencyController.IsPartitionActive(3), "consistencyController.IsPartitionActive")

	err = consistencyController.IsHealthy()
	if err == nil {
		t.Error("should be an error")
	}

	partitionZero := consistencyController.partitionsStates[0]

	partitionZero.ActivateEpoch(int64(1))
	partitionZero.ActivateEpoch(int64(2))
	partitionZero.ActivateEpoch(int64(3))
	assert.Equal(t, 3, partitionZero.GetActivateEpochs(), "partitionZero.GetActivateEpochs()")
	partitionZero.DeactivateEpoch(int64(3))

	assert.Equal(t, 2, partitionZero.GetActivateEpochs(), "partitionZero.GetActivateEpochs()")

	err = consistencyController.IsHealthy()
	if err == nil {
		t.Errorf("consistencyController.IsHealthy should have error. %v", err)
	}
}
