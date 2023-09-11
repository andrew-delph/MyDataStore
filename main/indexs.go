package main

import (
	"github.com/andrew-delph/my-key-store/storage"
)

func EpochIndex(parition, bucket, epoch int, key string) string {
	return storage.NewIndex("epoch").
		AddColumn(storage.CreateUnorderedColumn(string(parition))).
		AddColumn(storage.CreateUnorderedColumn(string(bucket))).
		AddColumn(storage.CreateOrderedColumn(epoch, 4)).
		AddColumn(storage.CreateUnorderedColumn(key)).
		Build()
}

func KeyIndex(key string) string {
	return storage.NewIndex("item").
		AddColumn(storage.CreateUnorderedColumn(key)).
		Build()
}
