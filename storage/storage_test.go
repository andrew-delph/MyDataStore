package storage

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
)

type StorageCallback func(t *testing.T, storage Storage)

func AllStorage(t *testing.T, storageCallback StorageCallback) {
	var storage Storage
	var err error
	c := config.GetConfig()

	err = os.RemoveAll(c.Storage.DataPath)
	if err != nil {
		t.Fatal(err)
	}

	storage = NewLevelDbStorage(c.Storage)
	storageCallback(t, storage)
	err = storage.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = os.RemoveAll(c.Storage.DataPath)
	if err != nil {
		t.Fatal(err)
	}
	storage = NewBadgerStorage(c.Storage)
	storageCallback(t, storage)
	err = storage.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestStorageSetGet(t *testing.T) {
	AllStorage(t, storageSetGet)
}

func storageSetGet(t *testing.T, storage Storage) {
	key := []byte("testkey")
	value := []byte("testvalue")
	err := storage.Put(key, value)
	if err != nil {
		t.Error(err)
	}
	res, err := storage.Get(key)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, value, res, "value should be equal")
	logrus.Info("value is equal")
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
	indexStr := fmt.Sprintf("%d", i)
	indexStr = fmt.Sprintf("testkey%s%s", strings.Repeat("0", 4-len(indexStr)), indexStr)
	return indexStr
}

func storageIterator(t *testing.T, storage Storage) {
	trx := storage.NewTransaction(true)
	defer trx.Discard()

	insertNum := 99
	for i := 0; i < insertNum; i++ {
		key := []byte(testIndex(i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		err := trx.Set(key, value)
		if err != nil {
			t.Error(err)
		}
	}

	err := trx.Commit()
	if err != nil {
		t.Error(err)
	}

	it := storage.NewIterator([]byte(testIndex(0)), []byte(testIndex(insertNum)))
	assert.EqualValues(t, true, it.First(), "it.First() should be true")

	count := 0

	for !it.isDone() {
		it.Next()
		count++
	}
	it.Release()
	assert.EqualValues(t, insertNum, count, "Should have iterated all inserted keys")

	startRange := 11
	endRange := 25
	it = storage.NewIterator([]byte(testIndex(startRange)), []byte(testIndex(endRange)))
	assert.EqualValues(t, true, it.First(), "it.First() should be true")

	count = 0
	for !it.isDone() {
		// logrus.Warn("key= ", string(it.Key()))
		it.Next()
		count++
	}
	it.Release()
	assert.EqualValues(t, endRange-startRange, count, "Should have iterated the range")
}
