package main

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
)

func badgerTest() {
	db, err := badger.Open(badger.DefaultOptions("/tmp1/badger"))
	if err != nil {
		logrus.Fatal(err)
	}
	defer db.Close()
}
