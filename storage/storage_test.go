package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
)

type StorageCallback func(t *testing.T, storage Storage)

func AllStorage(t *testing.T, storageCallback StorageCallback) {
	var storage Storage
	var err error
	var startTime time.Time
	var endTime time.Time
	var elapsedTime time.Duration
	c := config.GetConfig()

	c.Storage.DataPath = t.TempDir()

	storage = NewLevelDbStorage(c.Storage)
	startTime = time.Now()
	storageCallback(t, storage)
	endTime = time.Now()
	elapsedTime = endTime.Sub(startTime)
	err = storage.Close()
	if err != nil {
		t.Fatal(err)
	}
	logrus.Warnf("%s: [leveldb] elapsed: %v", t.Name(), elapsedTime)

	c.Storage.DataPath = t.TempDir()

	storage = NewBadgerStorage(c.Storage)
	startTime = time.Now()
	storageCallback(t, storage)
	endTime = time.Now()
	elapsedTime = endTime.Sub(startTime)
	logrus.Warnf("%s: [badger] elapsed: %v", t.Name(), elapsedTime)
	err = storage.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestStorageSetGet(t *testing.T) {
	AllStorage(t, storageSetGet)
}

func storageSetGet(t *testing.T, storage Storage) {
	_, err := storage.Get([]byte("not found"))
	assert.EqualValues(t, err, KEY_NOT_FOUND, "should error KEY_NOT_FOUND")
	return
	// key := []byte("testkey")
	// value := []byte("testvalue")
	// err := storage.Put(key, value)
	// if err != nil {
	// 	t.Error(err)
	// }
	// res, err := storage.Get(key)
	// if err != nil {
	// 	t.Error(err)
	// }
	// assert.EqualValues(t, value, res, "value should be equal")
}

func TestStorageTransaction(t *testing.T) {
	AllStorage(t, storageTransaction)
}

func storageTransaction(t *testing.T, storage Storage) {
	key := []byte("testkey")
	value := []byte("testvalue")

	trx := storage.NewTransaction(true)
	defer trx.Discard()

	err := trx.Set(key, value)
	if err != nil {
		t.Error(err)
	}
	err = trx.Commit()
	if err != nil {
		t.Error(err)
	}
	trx = storage.NewTransaction(false)
	defer trx.Discard()

	res, err := trx.Get(key)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, value, res, "value should be equal")
}

func TestStorageIterator(t *testing.T) {
	AllStorage(t, storageIterator)
}

func testIndex(i int) string {
	index, _ := NewIndex("test").
		AddColumn(CreateUnorderedColumn("partition", "1")).
		AddColumn(CreateOrderedColumn("key", fmt.Sprint(i), 4)).
		Build()
	return index
}

func storageIterator(t *testing.T, storage Storage) {
	trx := storage.NewTransaction(true)
	defer trx.Discard()

	writeValuesNum := 100
	if testing.Short() {
		writeValuesNum = 10
	}
	for i := 0; i < writeValuesNum; i++ {
		key := []byte(testIndex(i))
		value := []byte(fmt.Sprintf("%d", i))
		err := trx.Set(key, value)
		if err != nil {
			t.Error(err)
		}
	}
	err := trx.Commit()
	if err != nil {
		t.Error(err)
	}

	it := storage.NewIterator([]byte(testIndex(0)), []byte(testIndex(writeValuesNum)), false)
	assert.EqualValues(t, true, it.First(), "it.First() should be true")
	count := 0
	for !it.IsDone() {
		// logrus.Infof("count= %s value= %s", fmt.Sprintf("%d", count), string(it.Value()))
		assert.EqualValues(t, fmt.Sprintf("%d", count), string(it.Value()), "value should be ith")
		it.Next()
		count++
	}
	it.Release()
	assert.EqualValues(t, writeValuesNum, count, "Should have iterated all inserted keys")

	it = storage.NewIterator([]byte(testIndex(0)), []byte(testIndex(writeValuesNum)), true) // test in reverse
	assert.EqualValues(t, true, it.First(), "it.First() should be true")
	count = 0
	for !it.IsDone() {
		// logrus.Infof("count= %s value= %s", fmt.Sprintf("%d", count), string(it.Value()))
		assert.EqualValues(t, fmt.Sprintf("%d", writeValuesNum-count-1), string(it.Value()), "value should be ith")
		it.Next()
		count++
	}
	it.Release()
	assert.EqualValues(t, writeValuesNum, count, "Should have iterated all inserted keys")

	startRange := 11
	endRange := 25
	if writeValuesNum < endRange {
		t.Skip("skipping: writeValuesNum < endRange")
	}
	it = storage.NewIterator([]byte(testIndex(startRange)), []byte(testIndex(endRange)), false)
	assert.EqualValues(t, true, it.First(), "it.First() should be true")
	count = 0
	for !it.IsDone() {
		// logrus.Infof("count= %s value= %s", fmt.Sprintf("%d", startRange+count), string(it.Value()))
		assert.EqualValues(t, fmt.Sprintf("%d", startRange+count), string(it.Value()), "value should be ith")
		it.Next()
		count++
	}
	it.Release()
	assert.EqualValues(t, endRange-startRange, count, "Should have iterated the range")
}

func TestStorageBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	AllStorage(t, storageBenchmark)
}

func storageBenchmark(t *testing.T, storage Storage) {
	writeValuesNum := 100
	numGoroutines := 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			trx := storage.NewTransaction(true)
			defer trx.Discard()
			for i := 0; i < writeValuesNum; i++ {
				key := []byte(fmt.Sprintf("goroutine%d_", id) + testIndex(i))
				value := []byte(fmt.Sprintf("goroutine%d_", id) + fmt.Sprintf("%d", i))
				err := trx.Set(key, value)
				if err != nil {
					t.Error(err)
				}
			}
			err := trx.Commit()
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
}
