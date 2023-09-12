package main

import (
	"github.com/andrew-delph/my-key-store/storage"
)

func EpochIndex(parition int, bucket uint64, epoch int64, key string) string {
	return storage.NewIndex("epoch").
		AddColumn(storage.CreateUnorderedColumn(string(parition))).
		AddColumn(storage.CreateUnorderedColumn(string(bucket))).
		AddColumn(storage.CreateOrderedColumn(string(epoch), 4)).
		AddColumn(storage.CreateUnorderedColumn(key)).
		Build()
}

func KeyIndex(key string) string {
	return storage.NewIndex("item").
		AddColumn(storage.CreateUnorderedColumn(key)).
		Build()
}

func EpochTreeObjectIndex(partitionId int, epoch int64) string {
	return storage.NewIndex("epochtree").
		AddColumn(storage.CreateUnorderedColumn(string(partitionId))).
		AddColumn(storage.CreateUnorderedColumn(string(epoch))).
		Build()
}
