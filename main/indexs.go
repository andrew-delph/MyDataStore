package main

import (
	"strconv"

	"github.com/pkg/errors"

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

func ParseEpochIndex(indexStr string) (int, uint64, int64, string, error) {
	indexMap, err := storage.NewIndex("epoch").
		AddColumn(storage.CreateUnorderedColumn("parition", "1")).
		AddColumn(storage.CreateUnorderedColumn("bucket", "1")).
		AddColumn(storage.CreateOrderedColumn("epoch", "1", 4)).
		AddColumn(storage.CreateUnorderedColumn("key", "1")).Parse(indexStr)
	if err != nil {
		return 0, 0, 0, "", err
	}
	if indexMap["name"] != "epoch" {
		return 0, 0, 0, "", errors.New("index name is wrong")
	}
	parition, err := strconv.ParseInt(indexMap["parition"], 10, 64)
	if err != nil {
		return 0, 0, 0, "", err
	}
	bucket, err := strconv.ParseInt(indexMap["bucket"], 10, 64)
	if err != nil {
		return 0, 0, 0, "", err
	}
	epoch, err := strconv.ParseInt(indexMap["epoch"], 10, 64)
	if err != nil {
		return 0, 0, 0, "", err
	}
	key := indexMap["key"]
	return int(parition), uint64(bucket), epoch, key, nil
}

func BuildKeyIndex(key string) (string, error) {
	return storage.NewIndex("item").
		AddColumn(storage.CreateUnorderedColumn("key", key)).
		Build()
}

func BuildEpochTreeObjectIndex(partitionId int, epoch int64) (string, error) {
	return storage.NewIndex("epochtree").
		AddColumn(storage.CreateUnorderedColumn("partition", string(partitionId))).
		AddColumn(storage.CreateUnorderedColumn("epoch", string(epoch))).
		Build()
}
