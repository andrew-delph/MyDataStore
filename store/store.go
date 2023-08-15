package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"

	pb "github.com/andrew-delph/my-key-store/proto"
)

type Store interface {
	InitStore()
	setValue(value *pb.Value) error
	getValue(key string) (*pb.Value, bool, error)
	getPartition(partitionId int) (*cache.Cache, error)
	LoadPartitions(partitions []int)

	saveStore()
}

type GoCacheStore struct {
	partitionStore *cache.Cache
}

func NewGoCacheStore() GoCacheStore {
	return GoCacheStore{
		partitionStore: cache.New(0*time.Minute, 1*time.Minute),
	}
}

// Define a global cache variable

// Function to set a value in the global cache
func (store GoCacheStore) setValue(value *pb.Value) error {
	key := value.Key
	partitionId := FindPartitionID(events.consistent, key)
	partition, err := store.getPartition(partitionId)
	if partition == nil && err != nil {
		return err
	}

	existingValue, exists, err := store.getValue(key)
	if exists && existingValue.Epoch < value.Epoch {
		return fmt.Errorf("cannot set value with a lower Epoch. set = %d existing = %d", value.Epoch, existingValue.Epoch)
	}

	if exists && existingValue.UnixTimestamp < value.UnixTimestamp {
		return fmt.Errorf("cannot set value with a lower UnixTimestamp. set = %d existing = %d", value.UnixTimestamp, existingValue.UnixTimestamp)
	}
	partition.Set(key, value, 0)
	return nil
}

// Function to get a value from the global cache
func (store GoCacheStore) getValue(key string) (*pb.Value, bool, error) {
	partitionId := FindPartitionID(events.consistent, key)

	partition, err := store.getPartition(partitionId)
	if err != nil {
		return nil, false, err
	}
	if partition == nil {
		return nil, false, fmt.Errorf("partition is nil")
	}
	if value, found := partition.Get(key); found {
		if value, ok := value.(*pb.Value); ok {
			return value, true, nil
		}
	}
	return nil, false, nil
}

func (store GoCacheStore) getPartition(partitionId int) (*cache.Cache, error) {
	partitionKey := strconv.Itoa(partitionId)
	err := store.partitionStore.Add(partitionKey, cache.New(0*time.Minute, 1*time.Minute), 0)
	if err != nil {
		logrus.Debug(err)
	}
	if value, found := store.partitionStore.Get(partitionKey); found {
		partition, ok := value.(*cache.Cache)

		if ok {
			return partition, nil
		}
	}
	return nil, fmt.Errorf("partion not found: %d", partitionId)
}

func (store GoCacheStore) InitStore() {
	ticker := time.NewTicker(saveInterval)

	logrus.Debugf("store saveInterval %v", saveInterval)

	// Periodically save the cache to a file
	go func() {
		for range ticker.C {
			store.saveStore()
		}
	}()
}

func (store GoCacheStore) LoadPartitions(partitions []int) {
	for _, partitionId := range partitions {
		partitionFileName := fmt.Sprintf("/store/%s_%s.json", hostname, strconv.Itoa(partitionId))
		partition, err := store.getPartition(partitionId)
		if err != nil {
			logrus.Debugf("failed getPartition: %v , %v", partitionId, err)
			continue
		}

		err = partition.LoadFile(partitionFileName)
		if err != nil {
			logrus.Debugf("failed to load from file: %s : %v", partitionFileName, err)
		}
	}
}

func (store GoCacheStore) saveStore() {
	logrus.Debugf("Saving store: %s", hostname)

	for partitionId, value := range store.partitionStore.Items() {
		partition := value.Object.(*cache.Cache)
		partitionFileName := fmt.Sprintf("/store/%s_%s.json", hostname, partitionId)
		logrus.Debugf("saving partition to file %s", partitionFileName)
		err := partition.SaveFile(partitionFileName)
		if err != nil {
			logrus.Error(err)
		}
	}
}
