package storage

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
)

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
	res, exists, err := storage.Get(key)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, true, exists, "value should exist")
	assert.EqualValues(t, value, res, "value should be equal")
	logrus.Info("value is equal")
}

type StorageCallback func(t *testing.T, storage Storage)

func AllStorage(t *testing.T, storageCallback StorageCallback) {
	var storage Storage
	var err error
	c := config.GetConfig()

	err = os.RemoveAll(c.Storage.DataPath)
	if err != nil {
		logrus.Fatal(err)
	}

	storage = NewLevelDbStorage(c.Storage)
	storageCallback(t, storage)
	err = storage.Close()
	if err != nil {
		logrus.Fatal(err)
	}

	err = os.RemoveAll(c.Storage.DataPath)
	if err != nil {
		logrus.Fatal(err)
	}
	storage = NewBadgerStorage(c.Storage)
	storageCallback(t, storage)
	err = storage.Close()
	if err != nil {
		logrus.Fatal(err)
	}
}
