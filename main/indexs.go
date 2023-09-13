package main

import (
	"strconv"

	"github.com/andrew-delph/my-key-store/storage"
)

func BuildEpochIndex(parition int, bucket uint64, epoch int64, key string) (string, error) { // TODO create an itorator for lowerEpoch to upperEpoch
	return storage.NewIndex("epoch").
		AddColumn(storage.CreateUnorderedColumn("parition", strconv.FormatInt(int64(parition), 10))).
		AddColumn(storage.CreateUnorderedColumn("bucket", strconv.FormatUint(bucket, 10))).
		AddColumn(storage.CreateOrderedColumn("epoch", strconv.FormatInt(epoch, 10), 4)).
		AddColumn(storage.CreateUnorderedColumn("key", key)).
		Build()
}

func ParseEpochIndex(indexStr string) (map[string]string, error) {
	index := storage.NewIndex("epoch").
		AddColumn(storage.CreateUnorderedColumn("parition", "1")).
		AddColumn(storage.CreateUnorderedColumn("bucket", "1")).
		AddColumn(storage.CreateOrderedColumn("epoch", "1", 4)).
		AddColumn(storage.CreateUnorderedColumn("key", "1"))
	return index.Parse(indexStr)
}

func BuildKeyIndex(key string) string {
	index, _ := storage.NewIndex("item").
		AddColumn(storage.CreateUnorderedColumn("key", key)).
		Build()
	return index
}

func BuildEpochTreeObjectIndex(partitionId int, epoch int64) string {
	index, _ := storage.NewIndex("epochtree").
		AddColumn(storage.CreateUnorderedColumn("partition", string(partitionId))).
		AddColumn(storage.CreateUnorderedColumn("epoch", string(epoch))).
		Build()
	return index
}
