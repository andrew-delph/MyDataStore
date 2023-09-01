package main

import (
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	speedDir       = "/tmp/speed"
	speedValuesNum = 111
)

func cleanDir() {
	// Remove the existing Badger directory if it exists
	if _, err := os.Stat(speedDir); !os.IsNotExist(err) {
		err := os.RemoveAll(speedDir)
		if err != nil {
			log.Fatalf("Failed to remove directory: %v", err)
		}
	}
	// Create a new directory for Badger
	err := os.Mkdir(speedDir, 0755)
	if err != nil {
		log.Fatalf("Failed to create directory: %v", err)
	}
}

func TestSpeedBadgerSingle(t *testing.T) {
	cleanDir()

	defer trackTime(time.Now(), 0, "TestSpeedBadgerSingle")
	opts := badger.DefaultOptions(speedDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		logrus.Fatal(err)
	}
	defer db.Close()

	// Write 1000 key-value pairs to the database
	for i := 0; i < speedValuesNum; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)

		err := db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(key), []byte(value))
		})
		if err != nil {
			logrus.Fatal(err)
		}

	}
}

func TestSpeedBadgerBatch(t *testing.T) {
	cleanDir()

	defer trackTime(time.Now(), 0, "TestSpeedBadgerBatch")
	opts := badger.DefaultOptions(speedDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		logrus.Fatal(err)
	}
	defer db.Close()

	txn := db.NewTransaction(true) // 'true' indicates that this is a read-write transaction
	defer txn.Discard()

	// Write 1000 key-value pairs to the database
	for i := 0; i < speedValuesNum; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)

		if err := txn.Set([]byte(key), []byte(value)); err == badger.ErrTxnTooBig {
			err = txn.Commit()
			if err != nil {
				logrus.Fatal(err)
			}
			txn = db.NewTransaction(true)
			err = txn.Set([]byte(key), []byte(value))
			if err != nil {
				logrus.Fatal(err)
			}
		} else if err != nil {
			logrus.Fatal(err)
		}
	}
	err = txn.Commit()
	if err != nil {
		logrus.Fatal(err)
	}
}

func TestSpeedLevelDBSingle(t *testing.T) {
	cleanDir()
	defer trackTime(time.Now(), 0, "TestSpeedLevelDBSingle")

	writeOpts := &opt.WriteOptions{}
	writeOpts.Sync = false

	db, err := leveldb.OpenFile(speedDir, nil)
	if err != nil {
		logrus.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < speedValuesNum; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)

		err := db.Put([]byte(key), []byte(value), writeOpts)
		if err != nil {
			logrus.Fatal(err)
		}
	}
}

func TestSpeedLevelDBBatch(t *testing.T) {
	cleanDir()
	defer trackTime(time.Now(), 0, "TestSpeedLevelDBBatch")

	writeOpts := &opt.WriteOptions{}
	writeOpts.Sync = false

	db, err := leveldb.OpenFile(speedDir, nil)
	if err != nil {
		logrus.Fatal(err)
	}
	defer db.Close()

	batch := new(leveldb.Batch)

	// Add operations to the batch
	for i := 0; i < speedValuesNum; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		batch.Put([]byte(key), []byte(value))
	}
	err = db.Write(batch, nil)
	if err != nil {
		logrus.Fatal(err)
	}
}
