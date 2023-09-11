package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/reactivex/rxgo/v2"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"
)

func TestPartitionLocker(t *testing.T) {
	partitionLocker := NewPartitionLocker(10)
	assert.Equal(t, nil, partitionLocker.Lock(1), "should be nil")
	assert.NotEqual(t, nil, partitionLocker.Lock(1), "should be an error")
	assert.Equal(t, nil, partitionLocker.Unlock(1), "should be nil")
	assert.Equal(t, nil, partitionLocker.Lock(1), "should be nil")
}

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
	consistencyController := NewConsistencyController(10, nil) // TODO impllement proper test

	consistencyController.PublishEvent("test")
	time.Sleep(time.Second * 5)
}
