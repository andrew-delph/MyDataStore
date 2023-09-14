package storage

import (
	"errors"
	"time"

	"github.com/patrickmn/go-cache"
)

var KEY_NOT_FOUND = errors.New("KEY_NOT_FOUND")

type Storage interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	NewIterator(Start []byte, Limit []byte, reverse bool) Iterator
	NewTransaction(update bool) Transaction
	Close() error
}

type Iterator interface {
	First() bool
	Next() bool
	IsDone() bool
	Key() []byte
	Value() []byte
	Release()
}

type Transaction interface {
	Discard()
	Commit() error
	Set(key []byte, value []byte) error
	Get(key []byte) (value []byte, err error)
}

func cacheTest() {
	cache.New(0*time.Minute, 1*time.Minute)
}
