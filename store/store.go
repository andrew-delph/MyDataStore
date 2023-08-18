package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"google.golang.org/protobuf/proto"

	pb "github.com/andrew-delph/my-key-store/proto"
)

type Store interface {
	InitStore()
	setValue(value *pb.Value) error
	getValue(key string) (*pb.Value, bool, error)
	getPartition(partitionId int) (Partition, error)
	LoadPartitions(partitions []int)
	Items(partions []int, bucket, lowerEpoch, upperEpoch int) map[string]*pb.Value
	Close() error
	Clear()
}
type Partition interface {
	getValue(key string) (*pb.Value, bool, error)
	setValue(value *pb.Value) error
	Items(bucket, lowerEpoch, upperEpoch int) map[string]*pb.Value
	GetPartitionId() int
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

func NewGoCachePartition(partitionId int) Partition {
	partition := GoCachePartition{store: cache.New(0*time.Minute, 1*time.Minute)}
	partition.partitionId = partitionId
	return partition
}

func NewGoCacheStore() *GoCacheStore {
	return &GoCacheStore{
		partitionStore: cache.New(0*time.Minute, 1*time.Minute),
	}
}

type LevelDbStore struct {
	db *leveldb.DB
}

type LevelDbPartition struct {
	BasePartition
	db *leveldb.DB
}

func NewLevelDbPartition(db *leveldb.DB, partitionId int) Partition {
	partition := LevelDbPartition{db: db}
	partition.partitionId = partitionId
	return partition
}

func NewLevelDbStore() (*LevelDbStore, error) {
	storeFile := fmt.Sprintf("%s/data/%s", dataPath, hostname)
	db, err := leveldb.OpenFile(storeFile, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDbStore{db: db}, nil
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

func IndexBucketEpoch(parition, bucket, epoch int, key string) []byte {
	epochStr := fmt.Sprintf("%d", epoch)
	epochStr = fmt.Sprintf("%s%s", strings.Repeat("0", 4-len(epochStr)), epochStr)
	return []byte(fmt.Sprintf("%04d_%04d_%s_%s", parition, bucket, epochStr, key))
}

func IndexKey(paritionint int, key string) []byte {
	return []byte(fmt.Sprintf("real_%04d_%s", paritionint, key))
}

func (partition LevelDbPartition) setValue(value *pb.Value) error {
	writeOpts := &opt.WriteOptions{}
	writeOpts.Sync = true

	key := IndexKey(partition.GetPartitionId(), value.Key)
	data, err := proto.Marshal(value)
	if err != nil {
		logrus.Error("Error: ", err)
		return err
	}
	err = partition.db.Put(key, data, writeOpts)
	if err != nil {
		logrus.Error("Error: ", err)
		return err
	}
	bucketHash := CalculateHash(value.Key)
	bucket := bucketHash % partitionBuckets
	logrus.Debugf("setValue bucket %v", bucket)
	indexBytes := IndexBucketEpoch(partition.GetPartitionId(), bucket, int(value.Epoch), value.Key)

	// logrus.Error("indexBytes ", string(indexBytes))

	err = partition.db.Put(indexBytes, data, writeOpts)
	if err != nil {
		logrus.Error("Error: ", err)
		return err
	}

	return nil
}

func (partition LevelDbPartition) getValue(key string) (*pb.Value, bool, error) {
	keyBytes := IndexKey(partition.GetPartitionId(), key)
	valueBytes, err := partition.db.Get(keyBytes, nil)

	if err == leveldb.ErrNotFound {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}

	value := &pb.Value{}
	err = proto.Unmarshal(valueBytes, value)
	if err != nil {
		logrus.Error("Error: ", err)
		return nil, false, err
	}

	return value, true, nil
}

func (partition LevelDbPartition) Items(bucket, lowerEpoch, upperEpoch int) map[string]*pb.Value {
	itemsMap := make(map[string]*pb.Value)

	startRange, endRange := IndexBucketEpoch(partition.GetPartitionId(), bucket, lowerEpoch, ""), IndexBucketEpoch(partition.GetPartitionId(), bucket, upperEpoch, "")

	startRangeBytes := []byte(startRange)
	endRangeBytes := []byte(endRange)

	rng := &util.Range{Start: startRangeBytes, Limit: endRangeBytes}

	// Create an Iterator to iterate through the keys within the range
	readOpts := &opt.ReadOptions{}
	iter := partition.db.NewIterator(rng, readOpts)

	// iter = partition.db.NewIterator(nil, readOpts)
	defer iter.Release()

	for iter.Next() {
		key := string(iter.Key())
		valueBytes := iter.Value()
		logrus.Debugf("Items key: %s", key)
		value := &pb.Value{}
		err := proto.Unmarshal(valueBytes, value)
		if err != nil {
			logrus.Error("LevelDbPartition Items Unmarshal: ", err)
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

func (store LevelDbStore) Items(partions []int, bucket, lowerEpoch, upperEpoch int) map[string]*pb.Value {
	itemsMap := make(map[string]*pb.Value)

	for _, partitionId := range partions {

		partition, err := store.getPartition(partitionId)
		if err != nil {
			logrus.Error(err)
			continue
		}

		for key, item := range partition.Items(bucket, lowerEpoch, upperEpoch) {
			itemsMap[key] = item
		}
	}
	return itemsMap
}

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

// Define a global cache variable

// Function to set a value in the global cache
func (store LevelDbStore) setValue(value *pb.Value) error {
	key := value.Key
	partitionId := FindPartitionID(events.consistent, key)

	logrus.Debugf("setValue partitionId = %v", partitionId)

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
	return NewLevelDbPartition(store.db, partitionId), nil
}

func (store LevelDbStore) InitStore() {
	logrus.Debugf("InitStore for LevelDbStore")
}

func (store LevelDbStore) LoadPartitions(partitions []int) {
	// for _, partitionId := range partitions {
	// 	_, err := store.getPartition(partitionId)
	// 	if err != nil {
	// 		logrus.Debugf("failed getPartition: %v , %v", partitionId, err)
	// 		continue
	// 	}
	// }
}

func (store LevelDbStore) Close() error {
	return store.db.Close()
}

func (store LevelDbStore) Clear() {
	logrus.Fatal("LevelDbStore Clear not implemented.")
	// items := store.partitionStore.Items()

	// logrus.Warnf("Level Db Clear for partitions num %d", len(items))

	// for _, partitionObj := range items {
	// 	partition, ok := partitionObj.Object.(LevelDbPartition)
	// 	if ok {
	// 		// logrus.Warnf("Level Db Clear for partition %s", partitionId)
	// 		writeOpts := &opt.WriteOptions{Sync: true} // Optional: Sync data to disk

	// 		// Iterate through all keys and delete them
	// 		iter := partition.db.NewIterator(nil, nil)
	// 		for iter.Next() {
	// 			err := partition.db.Delete(iter.Key(), writeOpts)
	// 			if err != nil {
	// 				logrus.Error("Error deleting key:", err)
	// 			}
	// 		}
	// 		iter.Release()
	// 		err := iter.Error()
	// 		if err != nil {
	// 			logrus.Error("Iterator error:", err)
	// 		}
	// 	} else {
	// 		logrus.Error("FAILED TO Clear PARTITION")
	// 	}
	// }
}
