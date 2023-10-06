package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
)

func TestBadgerShutdown(t *testing.T) {
	var storage Storage
	var err error

	c := config.GetConfig()

	c.Storage.DataPath = t.TempDir()

	storage = NewBadgerStorage(c.Storage)

	key := []byte("testkey")
	value := []byte("testvalue")

	trx := storage.NewTransaction(true)

	err = trx.Set(key, value)
	if err != nil {
		t.Error(err)
	}

	err = trx.Commit()
	if err != nil {
		t.Error(err)
	}
	err = storage.Close()
	if err != nil {
		t.Error(err)
	}

	storage = NewBadgerStorage(c.Storage)
	res, err := storage.Get(key)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, value, res, "value should be equal")
}
