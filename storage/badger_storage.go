package storage

import (
	"bytes"

	"github.com/dgraph-io/badger"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
)

type BadgerStorage struct {
	db *badger.DB
}

func NewBadgerStorage(conf config.StorageConfig) BadgerStorage {
	ops := badger.DefaultOptions(conf.DataPath)
	ops.Logger = nil
	db, err := badger.Open(ops)
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

func (storage BadgerStorage) Get(key []byte) ([]byte, error) {
	txn := storage.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, KEY_NOT_FOUND
	} else if err != nil {
		return nil, err
	}

	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (storage BadgerStorage) NewTransaction(update bool) Transaction {
	trx := storage.db.NewTransaction(update)
	return BadgerTransaction{trx: trx}
}

func (storage BadgerStorage) Close() error {
	return storage.db.Close()
}

type BadgerIterator struct {
	it      *badger.Iterator
	trx     *badger.Txn
	Start   []byte
	Limit   []byte
	reverse bool
}

func (storage BadgerStorage) NewIterator(Start []byte, Limit []byte, reverse bool) Iterator {
	trx := storage.db.NewTransaction(false)
	op := badger.DefaultIteratorOptions
	op.Reverse = reverse
	it := trx.NewIterator(op)

	if reverse {
		it.Seek(Limit)
	} else {
		it.Seek(Start)
	}

	return BadgerIterator{it: it, trx: trx, Start: Start, Limit: Limit, reverse: reverse}
}

func (iterator BadgerIterator) First() bool {
	if iterator.reverse {
		iterator.it.Seek(iterator.Limit)
	} else {
		iterator.it.Seek(iterator.Start)
	}
	return iterator.it.Valid()
}

func (iterator BadgerIterator) Next() bool {
	iterator.it.Next()

	return !iterator.IsDone()
}

func (iterator BadgerIterator) IsDone() bool {
	return !iterator.it.Valid() || bytes.Compare(iterator.it.Item().Key(), iterator.Limit) >= 0
}

func (iterator BadgerIterator) Key() []byte {
	item := iterator.it.Item()
	return item.Key()
}

func (iterator BadgerIterator) Value() []byte {
	item := iterator.it.Item()
	value, err := item.ValueCopy(nil)
	if err != nil {
		logrus.Fatal(err)
	}
	return value
}

func (iterator BadgerIterator) Release() {
	iterator.it.Close()
	iterator.trx.Discard()
}

type BadgerTransaction struct {
	trx *badger.Txn
}

func (transaction BadgerTransaction) Discard() {
	transaction.trx.Discard()
}

func (transaction BadgerTransaction) Commit() error {
	return transaction.trx.Commit()
}

func (transaction BadgerTransaction) Set(key []byte, value []byte) error {
	return transaction.trx.Set(key, value)
}

func (transaction BadgerTransaction) Get(key []byte) ([]byte, error) {
	item, err := transaction.trx.Get(key)
	if err != nil {
		return nil, err
	}

	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return value, nil
}
