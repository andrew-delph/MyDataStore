package storage

import (
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/andrew-delph/my-key-store/config"
)

type LevelDbStorage struct {
	db *leveldb.DB
}

func NewLevelDbStorage(conf config.StorageConfig) LevelDbStorage {
	logrus.Warnf("Storage DataPath %s", conf.DataPath)
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

func (storage LevelDbStorage) Get(key []byte) ([]byte, bool, error) {
	readOpts := &opt.ReadOptions{}
	value, err := storage.db.Get(key, readOpts)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (storage LevelDbStorage) NewIterator(Start []byte, Limit []byte) Iterator {
	return LevelDbIterator{}
}

func (storage LevelDbStorage) Close() error {
	return storage.db.Close()
}

type LevelDbIterator struct{}

func (LevelDbIterator) First() bool {
	panic("not implemented") // TODO: Implement
}

func (LevelDbIterator) Next() bool {
	panic("not implemented") // TODO: Implement
}

func (LevelDbIterator) isDone() bool {
	panic("not implemented") // TODO: Implement
}

func (LevelDbIterator) Key() []byte {
	panic("not implemented") // TODO: Implement
}

func (LevelDbIterator) Value() []byte {
	panic("not implemented") // TODO: Implement
}

func (LevelDbIterator) Release() {
	panic("not implemented") // TODO: Implement
}
