package storage

import (
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/andrew-delph/my-key-store/config"
)

type LevelDbStorage struct {
	db *leveldb.DB
}

func NewLevelDbStorage(conf config.StorageConfig) LevelDbStorage {
	db, err := leveldb.OpenFile(conf.DataPath, nil)
	if err != nil {
		logrus.Fatal(err)
	}
	return LevelDbStorage{db: db}
}

func (storage LevelDbStorage) Put(key []byte, value []byte) error {
	writeOpts := &opt.WriteOptions{}
	writeOpts.Sync = true
	return storage.db.Put(key, value, writeOpts)
}

func (storage LevelDbStorage) Get(key []byte) ([]byte, error) {
	readOpts := &opt.ReadOptions{}
	value, err := storage.db.Get(key, readOpts)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (storage LevelDbStorage) NewIterator(Start []byte, Limit []byte) Iterator {
	rng := &util.Range{Start: Start, Limit: Limit}

	// Create an Iterator to iterate through the keys within the range
	readOpts := &opt.ReadOptions{}
	iter := storage.db.NewIterator(rng, readOpts)

	return LevelDbIterator{it: iter}
}

func (storage LevelDbStorage) Close() error {
	return storage.db.Close()
}

func (storage LevelDbStorage) NewTransaction(update bool) Transaction {
	return LevelDbTransaction{storage: storage}
}

type LevelDbIterator struct{ it iterator.Iterator }

func (iterator LevelDbIterator) First() bool {
	return iterator.it.First()
}

func (iterator LevelDbIterator) Next() bool {
	return iterator.it.Next()
}

func (iterator LevelDbIterator) IsDone() bool {
	return !iterator.it.Valid()
}

func (iterator LevelDbIterator) Key() []byte {
	return iterator.it.Key()
}

func (iterator LevelDbIterator) Value() []byte {
	return iterator.it.Value()
}

func (iterator LevelDbIterator) Release() {
	iterator.it.Release()
}

type LevelDbTransaction struct {
	storage LevelDbStorage
}

func (LevelDbTransaction) Discard() {
	return
}

func (LevelDbTransaction) Commit() error {
	return nil
}

func (trx LevelDbTransaction) Set(key []byte, value []byte) error {
	return trx.storage.Put(key, value)
}

func (trx LevelDbTransaction) Get(key []byte) (value []byte, err error) {
	return trx.storage.Get(key)
}
