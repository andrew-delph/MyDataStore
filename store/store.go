package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"

	pb "github.com/andrew-delph/my-key-store/datap"
)

type Store interface {
	InitStore()
	SetValue(value *pb.Value) error
	GetValue(key string) (*pb.Value, bool, error)
	getPartition(partitionId int) (Partition, error)
	LoadPartitions(partitions []int)
	Items(partions []int, bucket, lowerEpoch, upperEpoch int) map[string]*pb.Value
	Close() error
	Clear()
}
type Partition interface {
	GetPartitionValue(key string) (*pb.Value, bool, error)
	SetPartitionValue(value *pb.Value) error
	Items(bucket, lowerEpoch, upperEpoch int) map[string]*pb.Value
	GetPartitionId() int
	GetParitionEpochObject(epoch int) (*pb.ParitionEpochObject, error)
	PutParitionEpochObject(paritionEpochObject *pb.ParitionEpochObject) error
	LastParitionEpochObject() (*pb.ParitionEpochObject, error)
}

type GoCacheStore struct {
	partitionStore *cache.Cache
}

type BasePartition struct {
	partitionId int
}

func (partition BasePartition) GetPartitionId() int {
	return partition.partitionId
}

type GoCachePartition struct {
	BasePartition
	store *cache.Cache
}

func NewGoCacheStore() *GoCacheStore {
	return &GoCacheStore{
		partitionStore: cache.New(0*time.Minute, 1*time.Minute),
	}
}

func NewGoCachePartition(partitionId int) Partition {
	partition := GoCachePartition{store: cache.New(0*time.Minute, 1*time.Minute)}
	partition.partitionId = partitionId
	return partition
}

func (partition GoCachePartition) GetPartitionValue(key string) (*pb.Value, bool, error) {
	valueObj, exists := partition.store.Get(key)
	if exists {
		value, ok := valueObj.(*pb.Value)
		if ok {
			return value, true, nil
		}
	}
	return nil, false, nil
}

func (partition GoCachePartition) SetPartitionValue(value *pb.Value) error {
	partition.store.Set(value.Key, value, 0)
	return nil
}

func (partition GoCachePartition) Items(bucket, lowerEpoch, upperEpoch int) map[string]*pb.Value {
	// Create a new map of type map[string]*pb.Value
	itemsMap := make(map[string]*pb.Value)

	// Iterate over the original map and perform the conversion
	for key, item := range partition.store.Items() {

		value, ok := item.Object.(*pb.Value)

		if !ok {
			continue
		}
		itemsMap[key] = value
	}
	return itemsMap
}

// Define a global cache variable

func (store GoCacheStore) Close() error {
	return nil
}

func (store GoCacheStore) Clear() {
}

func (store GoCacheStore) Items(partions []int, bucket, lowerEpoch, upperEpoch int) map[string]*pb.Value {
	logrus.Fatal("not implemented.")
	// Create a new map of type map[string]*pb.Value
	itemsMap := make(map[string]*pb.Value)

	// // Iterate over the original map and perform the conversion
	// for key, item := range partition.store.Items() {

	// 	value, ok := item.Object.(*pb.Value)

	// 	if !ok {
	// 		continue
	// 	}
	// 	itemsMap[key] = value
	// }
	return itemsMap
}

// Function to set a value in the global cache
func (store GoCacheStore) SetValue(value *pb.Value) error {
	key := value.Key
	partitionId := FindPartitionID(events.consistent, key)
	partition, err := store.getPartition(partitionId)
	if partition == nil && err != nil {
		return err
	}

	existingValue, exists, err := store.GetValue(key)
	if exists && value.Epoch < existingValue.Epoch {
		return fmt.Errorf("cannot set value with a lower Epoch. set = %d existing = %d", value.Epoch, existingValue.Epoch)
	}

	if exists && existingValue.UnixTimestamp < value.UnixTimestamp {
		return fmt.Errorf("cannot set value with a lower UnixTimestamp. set = %d existing = %d", value.UnixTimestamp, existingValue.UnixTimestamp)
	}

	return partition.SetPartitionValue(value)
}

// Function to get a value from the global cache
func (store GoCacheStore) GetValue(key string) (*pb.Value, bool, error) {
	partitionId := FindPartitionID(events.consistent, key)

	partition, err := store.getPartition(partitionId)
	if err != nil {
		return nil, false, err
	}
	if partition == nil {
		return nil, false, fmt.Errorf("partition is nil")
	}
	value, found, err := partition.GetPartitionValue(key)
	if found {
		return value, true, nil
	}
	return nil, false, err
}

func (store GoCacheStore) getPartition(partitionId int) (Partition, error) {
	partitionKey := strconv.Itoa(partitionId)
	err := store.partitionStore.Add(partitionKey, NewGoCachePartition(partitionId), 0)
	if err != nil {
		logrus.Debug(err)
	}
	if value, found := store.partitionStore.Get(partitionKey); found {
		partition, ok := value.(Partition)

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
		partitionFileName := fmt.Sprintf("%s/%s_%s.json", dataPath, hostname, strconv.Itoa(partitionId))
		partition, err := store.getPartition(partitionId)
		if err != nil {
			logrus.Debugf("failed getPartition: %v , %v", partitionId, err)
			continue
		}

		goCachePartition, ok := (partition).(GoCachePartition)

		if !ok {
			continue
		}

		err = goCachePartition.store.LoadFile(partitionFileName)
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

func (partition GoCachePartition) GetParitionEpochObject(epoch int) (*pb.ParitionEpochObject, error) {
	return nil, nil
}

func (partition GoCachePartition) PutParitionEpochObject(paritionEpochObject *pb.ParitionEpochObject) error {
	return nil
}

func (partition GoCachePartition) LastParitionEpochObject() (*pb.ParitionEpochObject, error) {
	return nil, nil
}
