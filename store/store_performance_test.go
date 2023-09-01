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
)

var (
	speedDir       = "/tmp/speed"
	speedValuesNum = 1111111
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

func TestSpeedBadger(t *testing.T) {
	cleanDir()

	defer trackTime(time.Now(), 0, "TestSpeedBadger")
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
		}
	}
	err = txn.Commit()
	if err != nil {
		logrus.Fatal(err)
	}
}

func TestSpeedLevelDB(t *testing.T) {
	cleanDir()
	defer trackTime(time.Now(), 0, "TestSpeedLevelDB")

	db, err := leveldb.OpenFile(speedDir, nil)
	if err != nil {
		logrus.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < speedValuesNum; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)

		err := db.Put([]byte(key), []byte(value), nil)
		if err != nil {
			logrus.Fatal(err)
		}
	}
}
