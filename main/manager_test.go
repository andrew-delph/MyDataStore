package main

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/rpc"
)

func TestManagerDepsHolder(t *testing.T) {
	x := atomic.Bool{}
	logrus.Info("hi", x)
	logrus.Info(">", x.CompareAndSwap(false, false))
	logrus.Info(">", x.CompareAndSwap(false, false))
	// logrus.Info(">", x.CompareAndSwap(false, true))
	// logrus.Info(">", x.CompareAndSwap(true, false))
	// logrus.Info(">", x.CompareAndSwap(false, true))
	// logrus.Info(">", x.CompareAndSwap(false, false))
	assert.Equal(t, 1, 1, "always valid")
	// t.Error("")
}

func TestManagerStorage(t *testing.T) {
	var err error
	writeValuesNum := 100

	if testing.Short() {
		writeValuesNum = 10
		// t.Skip("skipping test in short mode.")
	}
	tmpDir := t.TempDir()
	logrus.Info("Temporary Directory:", tmpDir)
	c := config.GetConfig()
	c.Storage.DataPath = tmpDir
	c.Manager.PartitionCount = 1
	c.Manager.PartitionBuckets = 1

	manager := NewManager(c)

	// write to epoch 1
	for i := 0; i < writeValuesNum; i++ {
		k := fmt.Sprintf("key%d", i)
		v := fmt.Sprintf("val%d", i)
		setVal := &rpc.RpcValue{Key: k, Value: v, Epoch: 1}
		err = manager.SetValue(setVal)
		if err != nil {
			t.Error(err)
		}
		getVal, err := manager.GetValue(k)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, v, getVal.Value, "get value is wrong")
	}
	// write to epoch 2
	for i := 0; i < writeValuesNum; i++ {
		k := fmt.Sprintf("keyz%d", i)
		v := fmt.Sprintf("valz%d", i)
		setVal := &rpc.RpcValue{Key: k, Value: v, Epoch: 2}
		err = manager.SetValue(setVal)
		if err != nil {
			t.Error(err)
		}
		getVal, err := manager.GetValue(k)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, v, getVal.Value, "get value is wrong")
	}

	// check iterator for both...
	index1, err := BuildEpochIndex(0, 0, 1, "")
	if err != nil {
		t.Error(err)
	}
	index2, err := BuildEpochIndex(0, 0, 2, "")
	if err != nil {
		t.Error(err)
	}
	it := manager.db.NewIterator([]byte(index1), []byte(index2))
	assert.EqualValues(t, true, it.First(), "it.First() should be true")
	count := 0
	for !it.IsDone() {
		it.Next()
		count++
	}
	it.Release()
	assert.EqualValues(t, writeValuesNum, count, "Should have iterated all inserted keys")

	index3, err := BuildEpochIndex(0, 0, 1, "")
	if err != nil {
		t.Error(err)
	}
	index4, err := BuildEpochIndex(0, 0, 3, "")
	if err != nil {
		t.Error(err)
	}

	it = manager.db.NewIterator([]byte(index3), []byte(index4))
	assert.EqualValues(t, true, it.First(), "it.First() should be true")
	count = 0
	for !it.IsDone() {
		it.Next()
		count++
	}
	it.Release()
	assert.EqualValues(t, writeValuesNum*2, count, "Should have iterated all inserted keys")
}

func TestStreamBucketsTask(t *testing.T) {
	var err error
	writeValuesNum := 100

	if testing.Short() {
		writeValuesNum = 10
	}
	tmpDir := t.TempDir()
	logrus.Info("Temporary Directory:", tmpDir)
	c := config.GetConfig()
	c.Storage.DataPath = tmpDir
	c.Manager.PartitionCount = 1
	c.Manager.PartitionBuckets = 100

	manager := NewManager(c)

	// write to epoch 1
	for i := 0; i < writeValuesNum; i++ {
		k := fmt.Sprintf("key%d", i)
		v := fmt.Sprintf("val%d", i)
		setVal := &rpc.RpcValue{Key: k, Value: v, Epoch: 1}
		err = manager.SetValue(setVal)
		if err != nil {
			t.Error(err)
		}
		getVal, err := manager.GetValue(k)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, v, getVal.Value, "get value is wrong")
	}
	// write to epoch 2
	for i := 0; i < writeValuesNum; i++ {
		k := fmt.Sprintf("keyz%d", i)
		v := fmt.Sprintf("valz%d", i)
		setVal := &rpc.RpcValue{Key: k, Value: v, Epoch: 2}
		err = manager.SetValue(setVal)
		if err != nil {
			t.Error(err)
		}
		getVal, err := manager.GetValue(k)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, v, getVal.Value, "get value is wrong")
	}
	go manager.startWorker(1)
	// , Buckets: req.Buckets
	resCh := make(chan interface{})
	manager.reqCh <- rpc.StreamBucketsTask{PartitionId: int32(0), LowerEpoch: int64(0), UpperEpoch: int64(2), ResCh: resCh}
	itemCount := 0
outerLoop:
	for {
		select {
		case itemObj, ok := <-resCh:
			if !ok {
				logrus.Info("Channel is closed")
				break outerLoop
			}
			switch item := itemObj.(type) {
			case *rpc.RpcValue:
				logrus.Info("item ", item)
				itemCount++
			case error:
				t.Fatal(err)
			default:
				t.Fatalf("http unkown res type: %v", reflect.TypeOf(item))
			}
		}
	}
	assert.Equal(t, writeValuesNum, itemCount, "itemCount is wrong")

	resCh = make(chan interface{})
	manager.reqCh <- rpc.StreamBucketsTask{PartitionId: int32(0), LowerEpoch: int64(0), UpperEpoch: int64(3), ResCh: resCh}
	itemCount = 0
outerLoop2:
	for {
		select {
		case itemObj, ok := <-resCh:
			if !ok {
				logrus.Info("Channel is closed")
				break outerLoop2
			}
			switch item := itemObj.(type) {
			case *rpc.RpcValue:
				logrus.Info("item ", item)
				itemCount++
			case error:
				t.Fatal(err)
			default:
				t.Fatalf("http unkown res type: %v", reflect.TypeOf(item))
			}
		}
	}
	assert.Equal(t, writeValuesNum*2, itemCount, "itemCount is wrong")
}
