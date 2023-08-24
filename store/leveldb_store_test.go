package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"

	datap "github.com/andrew-delph/my-key-store/datap"
)

func TestExampleLevelDbIndex(t *testing.T) {
	db, err := leveldb.OpenFile(randomString(10), nil)
	defer db.Close()
	if err != nil {
		fmt.Println("Error creating/opening database:", err)
	}

	insertNum := 14

	curr := 0

	for e := 0; e < 100; e++ {
		for i := 0; i < insertNum; i++ {
			key := []byte(createTestKey(fmt.Sprintf("key%d", curr), 1, e))
			curr++
			value := []byte(fmt.Sprintf("value%d", i))
			err = db.Put(key, value, nil)
			if err != nil {
				fmt.Println("Error adding data to database:", err)
			}
		}
	}

	for i := 0; i < insertNum+7; i++ {
		key := []byte(createTestKey(fmt.Sprintf("key%d", i), 2, 1))
		value := []byte(fmt.Sprintf("value%d", i))
		err = db.Put(key, value, nil)
		if err != nil {
			fmt.Println("Error adding data to database:", err)
		}
	}

	for i := 0; i < insertNum+7; i++ {
		key := []byte(createTestKey(fmt.Sprintf("key%d", i), 3, 1))
		value := []byte(fmt.Sprintf("value%d", i))
		err = db.Put(key, value, nil)
		if err != nil {
			fmt.Println("Error adding data to database:", err)
		}
	}

	// Define start and end keys
	// start := []byte(createTestKey("key", 2, 0))
	// start := []byte(fmt.Sprintf("%04d_%04d", 2, 1))

	// start := []byte(fmt.Sprintf("%04d_%s", 1, epochString(1)))
	// end := []byte(fmt.Sprintf("%04d_%s", 1, epochString(2)))

	startRange, endRange := epochRange(0, 2)

	start := []byte(fmt.Sprintf("%04d_%s", 1, startRange))
	end := []byte(fmt.Sprintf("%04d_%s", 1, endRange))

	// Create a range that spans from start key to a key just after end key
	// rng := util.BytesPrefix(start)
	rng := &util.Range{Start: start, Limit: end}

	// Create an Iterator to iterate through the keys within the range
	iter := db.NewIterator(rng, nil)
	defer iter.Release()

	// Iterate through the keys within the range
	count := 0
	for iter.Next() {
		// key := iter.Key()
		// value := iter.Value()
		// fmt.Printf("Key: %s, Value: %s\n", key, value)
		// fmt.Printf("Key: %s\n", key)
		count++
	}

	fmt.Printf("\ncount: %d\n\n", count)

	fmt.Printf("\nRANGE: start = %s end = %s\n\n", string(start), string(end))

	assert.Equal(t, 0, count%insertNum, "insertNum and count should be equal")
	if err := iter.Error(); err != nil {
		fmt.Println("Iterator error:", err)
	}
}

func TestLevelDbStoreSingle(t *testing.T) {
	conf, delegate, events = GetConf()
	var err error
	store, err = NewLevelDbStore()
	if err != nil {
		t.Error(fmt.Sprintf("NewLevelDbStore: %v", err))
	}
	defer store.Close()

	err = store.SetValue(testValue("keyz", "value1", 1))
	if err != nil {
		t.Error(fmt.Sprintf("SetValue error: %v", err))
	}

	value, exists, err := store.GetValue("keyz")

	if !exists {
		t.Error(fmt.Sprintf("exists is false: %v", err))
		return
	}

	if err != nil {
		t.Error(fmt.Sprintf("error is not nil: %v", err))
		return
	}

	assert.Equal(t, "value1", value.Value, "Both should be SetMessage")
}

func TestLevelDbStoreSpeed(t *testing.T) {
	hostname = randomString(5)

	conf, delegate, events = GetConf()

	var err error
	store, err = NewLevelDbStore()
	if err != nil {
		t.Error(fmt.Sprintf("NewLevelDbStore: %v", err))
	}
	defer store.Close()

	startTime := time.Now()

	for i := 0; i < NumTestValues; i++ {
		store.SetValue(testValue(fmt.Sprintf("keyz%d", i), fmt.Sprintf("value%d", i), 1))
	}

	elapsedTime := time.Since(startTime).Seconds()

	fmt.Printf("TestLevelDbStoreSpeed Elapsed Time: %.2f seconds\n", elapsedTime)
}

func TestLevelDbIndex(t *testing.T) {
	hostname = randomString(5)

	conf, delegate, events = GetConf()

	var err error
	store, err = NewLevelDbStore()
	if err != nil {
		t.Error(fmt.Sprintf("NewLevelDbStore: %v", err))
		return
	}
	defer store.Close()

	testInsertNum := 300

	for i := 0; i < testInsertNum; i++ {
		err = store.SetValue(testValue(fmt.Sprintf("keyzds%d", i), fmt.Sprintf("value%d", i), 2))
		if err != nil {
			t.Errorf("SetValue: %v", err)
			return
		}
	}

	for i := 0; i < 33; i++ {
		err = store.SetValue(testValue(fmt.Sprintf("keyzx%d", i), fmt.Sprintf("value%d", i), 11))
		if err != nil {
			t.Errorf("SetValue: %v", err)
			return
		}
	}

	partitions := make([]int, partitionCount)

	for i := 0; i < partitionCount; i++ {
		partitions[i] = i
	}

	startTime := time.Now()

	allItemsMap := make(map[string]*datap.Value)

	for bucket := 0; bucket < partitionBuckets; bucket++ {
		// for bucket := 0; bucket < 1; bucket++ {
		items := store.Items(partitions, bucket, 2, 3)
		// logrus.Infof("bucket = %d Number of items: %d", bucket, len(items))

		for itemKey, itemValue := range items {
			allItemsMap[itemKey] = itemValue
		}
	}

	assert.Equal(t, testInsertNum, len(allItemsMap), "allItemsMap does not have correct amount")

	allItemsMap = make(map[string]*datap.Value)

	for bucket := 0; bucket < partitionBuckets; bucket++ {
		// for bucket := 0; bucket < 1; bucket++ {
		items := store.Items(partitions, bucket, 5, 20)
		// logrus.Infof("bucket = %d Number of items: %d", bucket, len(items))

		for itemKey, itemValue := range items {
			allItemsMap[itemKey] = itemValue
		}
	}

	assert.Equal(t, 33, len(allItemsMap), "allItemsMap does not have correct amount")

	elapsedTime := time.Since(startTime).Seconds()

	logrus.Infof("TestLevelDbIndex Elapsed Time: %.2f seconds\n", elapsedTime)
}

func TestLevelDbParitionEpochObject(t *testing.T) {
	hostname = randomString(5)

	conf, delegate, events = GetConf()

	var err error
	store, err = NewLevelDbStore()
	if err != nil {
		t.Error(fmt.Sprintf("NewLevelDbStore: %v", err))
		return
	}
	defer store.Close()

	partitionId := 5

	// get partition
	partition, err := store.getPartition(partitionId)
	if err != nil {
		logrus.Errorf("handlePartitionEpochItem err = %v", err)
		t.Error(err)
	}
	// check if last ParitionEpochObject exists. verify it doesnt.
	paritionEpochObject, err := partition.LastParitionEpochObject()
	if paritionEpochObject != nil {
		t.Errorf("should be nil: paritionEpochObject = %v err = %v", paritionEpochObject, err)
	}
	if err != STORE_NOT_FOUND {
		t.Error(err)
	}

	// input new object not valid ahead
	paritionEpochObject = &datap.ParitionEpochObject{
		Epoch:     5,
		Partition: int32(partitionId),
	}
	err = partition.PutParitionEpochObject(paritionEpochObject)
	if err != nil {
		t.Error(err)
	}

	// input new object valid
	paritionEpochObject = &datap.ParitionEpochObject{
		Epoch:     2,
		Partition: int32(partitionId),
		Valid:     true,
	}
	err = partition.PutParitionEpochObject(paritionEpochObject)
	if err != nil {
		t.Error(err)
	}

	// input new object not valid ahead
	paritionEpochObject = &datap.ParitionEpochObject{
		Epoch:     6,
		Partition: int32(partitionId),
	}
	err = partition.PutParitionEpochObject(paritionEpochObject)
	if err != nil {
		t.Error(err)
	}

	// check if last ParitionEpochObject exists. verify it exits.
	paritionEpochObject, err = partition.LastParitionEpochObject()
	if paritionEpochObject == nil {
		t.Fatalf("should exist: paritionEpochObject = %v err = %v", paritionEpochObject, err)
	}
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, partitionId, paritionEpochObject.Partition, "partition value was wrong.")
	assert.EqualValues(t, 2, paritionEpochObject.Epoch, "epoch value was wrong.")

	// input new higher object

	paritionEpochObject = &datap.ParitionEpochObject{
		Epoch:     4,
		Partition: int32(partitionId),
		Valid:     true,
	}
	err = partition.PutParitionEpochObject(paritionEpochObject)
	if err != nil {
		t.Error(err)
	}

	// make sure last returned is correct
	paritionEpochObject, err = partition.LastParitionEpochObject()
	if paritionEpochObject == nil {
		t.Fatalf("should be nil: paritionEpochObject = %v err = %v", paritionEpochObject, err)
	}
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, partitionId, paritionEpochObject.Partition, "partition value was wrong.")
	assert.EqualValues(t, 4, paritionEpochObject.Epoch, "epoch value was wrong.")

	// request first one specific and make sure it is returned
	paritionEpochObject, err = partition.GetParitionEpochObject(2)
	if paritionEpochObject == nil {
		t.Fatalf("should be nil: paritionEpochObject = %v err = %v", paritionEpochObject, err)
	}
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, partitionId, paritionEpochObject.Partition, "Partition value was wrong.")
	assert.EqualValues(t, 2, paritionEpochObject.Epoch, "Epoch value was wrong.")
	assert.EqualValues(t, true, paritionEpochObject.Valid, "Valid value was wrong.")

	paritionEpochObject, err = partition.GetParitionEpochObject(6)
	if paritionEpochObject == nil {
		t.Fatalf("should be nil: paritionEpochObject = %v err = %v", paritionEpochObject, err)
	}
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, partitionId, paritionEpochObject.Partition, "Partition value was wrong.")
	assert.EqualValues(t, 6, paritionEpochObject.Epoch, "Epoch value was wrong.")
	assert.EqualValues(t, false, paritionEpochObject.Valid, "Valid value was wrong.")

	paritionEpochObject, err = partition.GetParitionEpochObject(4)
	if paritionEpochObject == nil {
		t.Fatalf("should be nil: paritionEpochObject = %v err = %v", paritionEpochObject, err)
	}
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, partitionId, paritionEpochObject.Partition, "partition value was wrong.")
	assert.EqualValues(t, 4, paritionEpochObject.Epoch, "epoch value was wrong.")
}
