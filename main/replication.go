package main

import (
	"github.com/golang-collections/collections/set"
	"github.com/sirupsen/logrus"
)

// my partitions

// recalc my partitions
// new partitions
// old partitions
//
//	sync my partitions
func testSync() {
	logrus.Warn("SYNC.")
}

func HandleHashringChange() {
	mySet := set.New()
	logrus.Warn("mySet", mySet)
}

func VerifyPartition() {
}

func SyncPartition() {
}
