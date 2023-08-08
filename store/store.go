package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
)

// Define a global cache variable

var partitionStore *cache.Cache

// Function to set a value in the global cache
func setValue(partitionId int, key string, value string) error {
	partition, err := getPartition(partitionId)
	if partition == nil && err != nil {
		return err
	}
	partition.Set(key, value, 0)
	return nil
}

// Function to get a value from the global cache
func getValue(partitionId int, key string) (string, bool, error) {
	partition, err := getPartition(partitionId)
	if partition == nil && err != nil {
		return "", false, err
	}
	if value, found := partition.Get(key); found {
		if strValue, ok := value.(string); ok {
			return strValue, true, nil
		}
	}
	return "", false, nil
}

func getPartition(partitionId int) (*cache.Cache, error) {
	partitionKey := strconv.Itoa(partitionId)
	err := partitionStore.Add(partitionKey, cache.New(0*time.Minute, 1*time.Minute), 0)
	if err != nil {
		logrus.Debug(err)
	}
	if value, found := partitionStore.Get(partitionKey); found {
		partition, ok := value.(*cache.Cache)

		if ok {
			return partition, nil
		}
	}
	return nil, fmt.Errorf("partion not found: %d", partitionId)
}

func InitStore() {
	partitionStore = cache.New(0*time.Minute, 1*time.Minute)
	ticker := time.NewTicker(saveInterval)

	logrus.Warnf("store saveInterval %v", saveInterval)

	// Periodically save the cache to a file
	go func() {
		for range ticker.C {
			saveStore()
		}
	}()
}

func LoadPartitions(partitions []int) {

	for _, partitionId := range partitions {
		partitionFileName := fmt.Sprintf("/store/%s_%s.json", hostname, strconv.Itoa(partitionId))
		partition, err := getPartition(partitionId)
		if err != nil {
			logrus.Errorf("failed getPartition: %v , %v", partitionId, err)
			continue
		}

		err = partition.LoadFile(partitionFileName)
		if err != nil {
			logrus.Errorf("failed to load from file: %s : %v", partitionFileName, err)
		}
	}

}

func saveStore() {
	logrus.Warn("Saving store.")

	for partitionId, value := range partitionStore.Items() {
		partition := value.Object.(*cache.Cache)
		partitionFileName := fmt.Sprintf("/store/%s_%s.json", hostname, partitionId)
		logrus.Warnf("saving partition to file %s", partitionFileName)
		err := partition.SaveFile(partitionFileName)
		if err != nil {
			logrus.Error(err)
		}
	}
}
