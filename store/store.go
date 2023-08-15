package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/protobuf/proto"

	pb "github.com/andrew-delph/my-key-store/proto"
)

type Store interface {
	InitStore()
	setValue(value *pb.Value) error
	getValue(key string) (*pb.Value, bool, error)
	getPartition(partitionId int) (Partition, error)
	LoadPartitions(partitions []int)
}
type Partition interface {
	getValue(key string) (*pb.Value, bool, error)
	setValue(value *pb.Value) error
	Items() map[string]*pb.Value
}

type GoCacheStore struct {
	partitionStore *cache.Cache
}

type GoCachePartition struct {
	store *cache.Cache
}

type LevelDbPartition struct {
	db *leveldb.DB
}

func NewGoCachePartition() Partition {
	return GoCachePartition{store: cache.New(0*time.Minute, 1*time.Minute)}
}

func (partition GoCachePartition) getValue(key string) (*pb.Value, bool, error) {
	valueObj, exists := partition.store.Get(key)
	if exists {
		value, ok := valueObj.(*pb.Value)
		if ok {
			return value, true, nil
		}
	}
	return nil, false, nil
}

func (partition GoCachePartition) setValue(value *pb.Value) error {
	partition.store.Set(value.Key, value, 0)
	return nil
}

func (partition GoCachePartition) Items() map[string]*pb.Value {
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

func (partition LevelDbPartition) getValue(key string) (*pb.Value, bool, error) {
	keyBytes := []byte(key)
	valueBytes, err := partition.db.Get(keyBytes, nil)
	if err != nil {
		return nil, false, err
	}

	if valueBytes == nil {
		return nil, false, nil
	}

	value := &pb.Value{}

	err = proto.Unmarshal(valueBytes, value)
	if err != nil {
		logrus.Error("Error: ", err)
		return nil, false, err
	}

	return value, true, nil
}

func (partition LevelDbPartition) setValue(value *pb.Value) error {
	key := []byte(value.Key)
	data, err := proto.Marshal(value)
	if err != nil {
		logrus.Error("Error: ", err)
		return err
	}
	err = partition.db.Put(key, data, nil)
	if err != nil {
		logrus.Error("Error: ", err)
		return err
	}
	return nil
}

func (partition LevelDbPartition) Items() map[string]*pb.Value {
	// Create a new map of type map[string]*pb.Value
	// itemsMap := make(map[string]*pb.Value)

	// // Iterate over the original map and perform the conversion
	// for key, item := range partition.store.Items() {

	// 	value, ok := item.Object.(*pb.Value)

	// 	if !ok {
	// 		continue
	// 	}
	// 	itemsMap[key] = value
	// }
	// return itemsMap
	logrus.Fatal("LevelDbPartition.Items() no implemented.")
	return nil
}

func NewLevelDbPartition(partitionKey string) (Partition, error) {
	db, err := leveldb.OpenFile(partitionKey, nil)
	if err != nil {
		return nil, err
	}
	return LevelDbPartition{db: db}, nil
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
	partition.setValue(value)
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
	value, found, err := partition.getValue(key)
	if found {
		return value, true, nil
	}
	return nil, false, err
}

func (store GoCacheStore) getPartition(partitionId int) (Partition, error) {
	partitionKey := strconv.Itoa(partitionId)
	err := store.partitionStore.Add(partitionKey, NewGoCachePartition(), 0)
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
		partitionFileName := fmt.Sprintf("/store/%s_%s.json", hostname, strconv.Itoa(partitionId))
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

type LevelDbStore struct {
	partitionStore *cache.Cache
}

func NewLevelDbStore() LevelDbStore {
	return LevelDbStore{partitionStore: cache.New(0*time.Minute, 1*time.Minute)}
}

// Define a global cache variable

// Function to set a value in the global cache
func (store LevelDbStore) setValue(value *pb.Value) error {
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

	return partition.setValue(value)
}

// Function to get a value from the global cache
func (store LevelDbStore) getValue(key string) (*pb.Value, bool, error) {
	partitionId := FindPartitionID(events.consistent, key)

	partition, err := store.getPartition(partitionId)
	if err != nil {
		return nil, false, err
	}
	if partition == nil {
		return nil, false, fmt.Errorf("partition is nil")
	}
	value, found, err := partition.getValue(key)
	if found {
		return value, true, nil
	}
	return nil, false, err
}

func (store LevelDbStore) getPartition(partitionId int) (Partition, error) {
	partitionKey := fmt.Sprintf("%s_%d", hostname, partitionId)

	if value, found := store.partitionStore.Get(partitionKey); found {
		partition, ok := value.(Partition)

		if ok {
			return partition, nil
		}
	}

	db, err := NewLevelDbPartition(partitionKey)

	if err != nil {
		logrus.Debug(err)
	} else {
		err = store.partitionStore.Add(partitionKey, db, 0)
		if err != nil {
			logrus.Debug(err)
		}
	}

	if value, found := store.partitionStore.Get(partitionKey); found {
		partition, ok := value.(Partition)

		if ok {
			return partition, nil
		}
	}
	return nil, fmt.Errorf("partion not found: %d", partitionId)
}

func (store LevelDbStore) InitStore() {
	logrus.Debugf("InitStore for LevelDbStore")
}

func (store LevelDbStore) LoadPartitions(partitions []int) {
	for _, partitionId := range partitions {
		_, err := store.getPartition(partitionId)
		if err != nil {
			logrus.Debugf("failed getPartition: %v , %v", partitionId, err)
			continue
		}
	}
}
