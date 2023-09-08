package main

import (
	"github.com/golang-collections/collections/set"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/hashring"
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

func HandleHashringChange(ring *hashring.Hashring) {
	logrus.Warn(ring.GetMyPartions())
	mySet := set.New()
	logrus.Warn("mySet", mySet)
}

func VerifyPartition() {
}

func SyncPartition() {
}
