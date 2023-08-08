package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/cbergoon/merkletree"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
)

// Define a global cache variable

var partitionStore *cache.Cache = cache.New(0*time.Minute, 1*time.Minute)

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

type MerkleContent struct {
	key   string
	value string
}

func (content MerkleContent) CalculateHash() ([]byte, error) {
	h := md5.New()
	if _, err := h.Write([]byte(content.key + content.value)); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

func (content MerkleContent) Equals(other merkletree.Content) (bool, error) {
	otherTC, ok := other.(MerkleContent)
	if !ok {
		return false, errors.New("value is not of type MerkleContent")
	}
	return content.key == otherTC.key && content.value == otherTC.value, nil
}

func PartitionMerkleTree(partitionId int) (*merkletree.MerkleTree, error) {
	partition, err := getPartition(partitionId)
	if err != nil {
		logrus.Debug(err)
		return nil, err
	}

	items := partition.Items()

	// Extract keys and sort them
	var keys []string
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Build content list in sorted order of keys
	var contentList []merkletree.Content
	for _, key := range keys {
		valueObj := items[key]
		value := valueObj.Object.(string)
		logrus.Infof("item %s %s", key, value)
		contentList = append(contentList, MerkleContent{key: key, value: value})
	}

	return merkletree.NewTree(contentList)
}

func InitStore() {
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
	logrus.Debugf("Saving store: %s", hostname)

	for partitionId, value := range partitionStore.Items() {
		partition := value.Object.(*cache.Cache)
		partitionFileName := fmt.Sprintf("/store/%s_%s.json", hostname, partitionId)
		logrus.Debugf("saving partition to file %s", partitionFileName)
		err := partition.SaveFile(partitionFileName)
		if err != nil {
			logrus.Error(err)
		}
	}
}
