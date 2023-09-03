package storage

import (
	"time"

	"github.com/dgraph-io/badger"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/andrew-delph/my-key-store/config"
)

type Storage interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	NewIterator(Start []byte, Limit []byte) Iterator
	Close() error
}

type Iterator interface {
	First() bool
	Next() bool
	isDone() bool
	Key() []byte
	Value() []byte
	Release()
}

func Value() string {
	c := config.GetConfig()
	badgerTest()
	leveldbTest()
	cacheTest()
	NewLevelDbStorage(c.Storage)

	return "test"
}

func badgerTest() {
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	defer db.Close()
	if err != nil {
		logrus.Fatal(err)
	}
}

func leveldbTest() {
	db, err := leveldb.OpenFile("/tmp/level", nil)
	defer db.Close()
	if err != nil {
		logrus.Fatal(err)
	}
}

func cacheTest() {
	cache.New(0*time.Minute, 1*time.Minute)
}
