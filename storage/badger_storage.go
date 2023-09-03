package storage

import (
	"github.com/dgraph-io/badger"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
)

type BadgerStorage struct {
	db *badger.DB
}

func NewBadgerStorage(conf config.StorageConfig) BadgerStorage {
	db, err := badger.Open(badger.DefaultOptions(conf.DataPath))
	if err != nil {
		logrus.Fatal(err)
	}
	return BadgerStorage{db: db}
}

func (storage BadgerStorage) Put(key []byte, value []byte) error {
	txn := storage.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Set(key, value)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (storage BadgerStorage) Get(key []byte) ([]byte, bool, error) {
	txn := storage.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(key)
	if err != nil {
		return nil, false, err
	}

	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, false, err
	}

	return value, true, nil
}

func (storage BadgerStorage) NewIterator(Start []byte, Limit []byte) Iterator {
	return LevelDbIterator{}
}

func (storage BadgerStorage) Close() error {
	return storage.db.Close()
}

type BadgerIterator struct{}

func (BadgerIterator) First() bool {
	panic("not implemented") // TODO: Implement
}

func (BadgerIterator) Next() bool {
	panic("not implemented") // TODO: Implement
}

func (BadgerIterator) isDone() bool {
	panic("not implemented") // TODO: Implement
}

func (BadgerIterator) Key() []byte {
	panic("not implemented") // TODO: Implement
}

func (BadgerIterator) Value() []byte {
	panic("not implemented") // TODO: Implement
}

func (BadgerIterator) Release() {
	panic("not implemented") // TODO: Implement
}
