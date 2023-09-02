package main

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"google.golang.org/protobuf/proto"

	datap "github.com/andrew-delph/my-key-store/datap"
)

type LevelDbStore struct {
	db *leveldb.DB
}

type LevelDbPartition struct {
	BasePartition
	db *leveldb.DB
}

func IndexBucketEpoch(parition, bucket, epoch int, key string) []byte {
	epochStr := fmt.Sprintf("%d", epoch)
	epochStr = fmt.Sprintf("%s%s", strings.Repeat("0", 4-len(epochStr)), epochStr)
	return []byte(fmt.Sprintf("%04d_%04d_%s_%s", parition, bucket, epochStr, key))
}

func IndexPartitionEpochObject(parition, epoch int) []byte {
	return []byte(fmt.Sprintf("%s_%04d", IndexPartitionEpochObject_Partition_Only(parition), epoch))
}

func IndexPartitionEpochObject_Partition_Only(parition int) []byte {
	return []byte(fmt.Sprintf("epoch_object_%04d", parition))
}

func IndexKey(paritionint int, key string) []byte {
	return []byte(fmt.Sprintf("real_%04d_%s", paritionint, key))
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

func (partition LevelDbPartition) SetPartitionValue(value *datap.Value) error {
	writeOpts := &opt.WriteOptions{}
	writeOpts.Sync = false

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
	logrus.Debugf("SetValue p = %v b = %v", partition.GetPartitionId(), bucket)
	indexBytes := IndexBucketEpoch(partition.GetPartitionId(), bucket, int(value.Epoch), value.Key)

	err = partition.db.Put(indexBytes, data, writeOpts)
	if err != nil {
		logrus.Errorf("Error: %v", err)
		return err
	}

	return nil
}

func (partition LevelDbPartition) GetPartitionValue(key string) (*datap.Value, bool, error) {
	keyBytes := IndexKey(partition.GetPartitionId(), key)
	valueBytes, err := partition.db.Get(keyBytes, nil)

	if err == leveldb.ErrNotFound {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}

	value := &datap.Value{}
	err = proto.Unmarshal(valueBytes, value)
	if err != nil {
		logrus.Error("Error: ", err)
		return nil, false, err
	}

	return value, true, nil
}

func (partition LevelDbPartition) Items(bucket, lowerEpoch, upperEpoch int) map[string]*datap.Value {
	itemsMap := make(map[string]*datap.Value)

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
		value := &datap.Value{}
		err := proto.Unmarshal(valueBytes, value)
		if err != nil {
			logrus.Error("LevelDbPartition Items Unmarshal: ", err)
			continue
		}

		itemsMap[key] = value
	}
	return itemsMap
}

func (store LevelDbStore) Items(partions []int, bucket, lowerEpoch, upperEpoch int) map[string]*datap.Value {
	itemsMap := make(map[string]*datap.Value)

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
func (store LevelDbStore) SetValue(value *datap.Value) error {
	key := value.Key
	partitionId := FindPartitionID(events.consistent, key)

	logrus.Debugf("SetValue partitionId = %v", partitionId)

	partition, err := store.getPartition(partitionId)
	if partition == nil && err != nil {
		return err
	}

	existingValue, exists, err := partition.GetPartitionValue(key)
	if exists && value.Epoch < existingValue.Epoch {
		return fmt.Errorf("cannot set value with a lower Epoch. set = %d existing = %d", value.Epoch, existingValue.Epoch)
	}

	if exists && existingValue.UnixTimestamp < value.UnixTimestamp {
		return fmt.Errorf("cannot set value with a lower UnixTimestamp. set = %d existing = %d", value.UnixTimestamp, existingValue.UnixTimestamp)
	}

	return partition.SetPartitionValue(value)
}

// Function to get a value from the global cache
func (store LevelDbStore) GetValue(key string) (*datap.Value, bool, error) {
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

func (partition LevelDbPartition) GetPartitionEpochObject(epoch int) (*datap.PartitionEpochObject, error) {
	keyBytes := IndexPartitionEpochObject(partition.GetPartitionId(), epoch)
	valueBytes, err := partition.db.Get(keyBytes, nil)
	if err == leveldb.ErrNotFound {
		return nil, STORE_NOT_FOUND
	}
	if err != nil {
		return nil, err
	}

	partitionEpochObject := &datap.PartitionEpochObject{}
	err = proto.Unmarshal(valueBytes, partitionEpochObject)
	if err != nil {
		logrus.Error("Error: ", err)
		return nil, err
	}

	return partitionEpochObject, nil
}

func (partition LevelDbPartition) PutPartitionEpochObject(partitionEpochObject *datap.PartitionEpochObject) error {
	writeOpts := &opt.WriteOptions{}
	writeOpts.Sync = true

	key := IndexPartitionEpochObject(partition.GetPartitionId(), int(partitionEpochObject.Epoch))
	data, err := proto.Marshal(partitionEpochObject)
	if err != nil {
		logrus.Error("Error: ", err)
		return err
	}
	err = partition.db.Put(key, data, writeOpts)
	if err != nil {
		logrus.Error("Error: ", err)
		return err
	}
	return nil
}

func (partition LevelDbPartition) LastPartitionEpochObject() (*datap.PartitionEpochObject, error) {
	keyBytes := IndexPartitionEpochObject_Partition_Only(partition.GetPartitionId())

	rng := util.BytesPrefix(keyBytes)
	readOpts := &opt.ReadOptions{}

	iter := partition.db.NewIterator(rng, readOpts)

	defer iter.Release()

	for ok := iter.Last(); ok; ok = iter.Prev() {
		valueBytes := iter.Value()
		partitionEpochObject := &datap.PartitionEpochObject{}
		err := proto.Unmarshal(valueBytes, partitionEpochObject)
		if err != nil {
			return nil, err
		}
		if partitionEpochObject.Valid {
			return partitionEpochObject, nil
		}

	}
	return nil, STORE_NOT_FOUND
}
