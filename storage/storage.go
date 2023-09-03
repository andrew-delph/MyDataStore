package storage

import (
	"time"

	"github.com/dgraph-io/badger"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
)

func Value() string {
	badgerTest()
	leveldbTest()
	cacheTest()

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
