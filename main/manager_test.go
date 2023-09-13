package main

import (
	"fmt"
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

func createMockManager(t *testing.T) Manager {
	tmpDir := t.TempDir()
	logrus.Info("Temporary Directory:", tmpDir)
	c := config.GetConfig()
	c.Storage.DataPath = tmpDir
	c.Manager.PartitionCount = 1
	c.Manager.PartitionBuckets = 1

	manager := NewManager(c)
	return manager
}

func TestManagerStorage(t *testing.T) {
	var err error
	// if testing.Short() {
	// 	t.Skip("skipping test in short mode.")
	// }
	manager := createMockManager(t)

	writeValuesNum := 10

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
	// write to epoch 1
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

	it := manager.db.NewIterator([]byte(EpochIndex(0, 0, 1, "")), []byte(EpochIndex(0, 0, 2, "")))
	assert.EqualValues(t, true, it.First(), "it.First() should be true")
	count := 0
	for !it.IsDone() {
		it.Next()
		count++
	}
	it.Release()
	assert.EqualValues(t, writeValuesNum, count, "Should have iterated all inserted keys")

	it = manager.db.NewIterator([]byte(EpochIndex(0, 0, 1, "")), []byte(EpochIndex(0, 0, 3, "")))
	assert.EqualValues(t, true, it.First(), "it.First() should be true")
	count = 0
	for !it.IsDone() {
		it.Next()
		count++
	}
	it.Release()
	assert.EqualValues(t, writeValuesNum*2, count, "Should have iterated all inserted keys")
}
