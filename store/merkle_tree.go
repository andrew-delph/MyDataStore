package main

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/cbergoon/merkletree"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	pb "github.com/andrew-delph/my-key-store/proto"
)

var hashMod = int64(999999)

type CustomHash struct {
	value int64
}

func (h *CustomHash) Add(b []byte) {
	sum := int64(0)
	for _, value := range b {
		sum += int64(value)
	}
	h.value += sum
	h.value = h.value % hashMod
}

func (h *CustomHash) Remove(b []byte) {
	sum := int64(0)
	for _, value := range b {
		sum += int64(value)
	}
	h.value -= sum
	for h.value < 0 {
		h.value += hashMod
	}
	h.value = h.value % hashMod
}

func (h *CustomHash) Hash() int64 {
	return h.value
}

var merkletreeStore *cache.Cache = cache.New(0*time.Minute, 1*time.Minute)

type MerkleBucket struct {
	hasher   *CustomHash
	bucketId int32
}

func (bucket MerkleBucket) CalculateHash() ([]byte, error) {
	return EncodeInt64ToBytes(bucket.hasher.Hash())
}

func (bucket MerkleBucket) AddValue(value *pb.Value) error {
	data, err := proto.Marshal(value)
	if err != nil {
		logrus.Error("Error: ", err)
		return err
	}
	bucket.hasher.Add(data)
	return nil
}

func (bucket MerkleBucket) RemoveValue(value *pb.Value) error {
	data, err := proto.Marshal(value)
	if err != nil {
		logrus.Error("Error: ", err)
		return err
	}
	bucket.hasher.Remove(data)
	return nil
}

func (content MerkleBucket) Equals(other merkletree.Content) (bool, error) {
	otherTC, ok := other.(MerkleBucket)
	if !ok {
		return false, errors.New("value is not of type MerkleContent")
	}
	return content.hasher.Hash() == otherTC.hasher.Hash(), nil
}

var (
	bucketEpochLag        = 3
	currGlobalBucketEpoch int64
)

// bucketsMap is  [epoch][partition][bucket]
var (
	bucketsMap   = make(map[int64]map[int][]*MerkleBucket)
	globalBucket = NewBucketsHolder()
)

func AddBucket(epoch int64, partitionId, bucket int, value *pb.Value) error {
	if epoch > currGlobalBucketEpoch {
		//
	} else {
		globalBucket[partitionId][bucket].RemoveValue(value)
	}
	currBucket, err := GetBucket(epoch, partitionId, bucket)
	if err != nil {
		return err
	}
	err = currBucket.AddValue(value)
	if err != nil {
		logrus.Warnf("currBucket.AddValue err = %v", err)
	}
	return nil
}

func RemoveBucket(epoch int64, partitionId, bucket int, value *pb.Value) error {
	if epoch > currGlobalBucketEpoch {
		//
	} else {
		globalBucket[partitionId][bucket].AddValue(value)
	}
	currBucket, err := GetBucket(epoch, partitionId, bucket)
	if err != nil {
		return err
	}
	err = currBucket.RemoveValue(value)
	if err != nil {
		logrus.Warnf("currBucket.AddValue err = %v", err)
	}
	return nil
}

func GetBucket(epoch int64, partitionId, bucket int) (*MerkleBucket, error) {
	if bucketHolder, ok := bucketsMap[epoch]; ok {
		return bucketHolder[partitionId][bucket], nil
	}
	keys := make([]int64, bucketEpochLag)
	for i := range bucketsMap {
		keys = append(keys, i)
	}
	return nil, fmt.Errorf("Bucket does not exist epoch = %d partitionId = %d bucket = %d currGlobalBucketEpoch = %d len(bucketsMap) = %d keys = %v", epoch, partitionId, bucket, currGlobalBucketEpoch, len(bucketsMap), keys)
}

func UpdateGlobalBucket(newEpoch int64) {
	logrus.Warnf("UpdateGlobalBucket newEpoch = %d", newEpoch)
	bucketsMap[newEpoch] = NewBucketsHolder()
	for len(bucketsMap) > bucketEpochLag {
		minBucket := int64(math.MaxInt64)
		for bucket := range bucketsMap {
			minBucket = min(minBucket, bucket)
			min(minBucket, bucket)
		}
		logrus.Warnf("Delete Buckets on Epoch = %v", minBucket)
		delete(bucketsMap, minBucket)
	}
	currGlobalBucketEpoch = newEpoch - 1
}

func NewBucketsHolder() map[int][]*MerkleBucket {
	bucketHolder := make(map[int][]*MerkleBucket, partitionCount)
	for i := 0; i < partitionCount; i++ {
		bucketHolder[i] = make([]*MerkleBucket, partitionBuckets)
		for j := 0; j < partitionBuckets; j++ {
			bucketHolder[i][j] = &MerkleBucket{hasher: &CustomHash{}}
		}
	}
	return bucketHolder
}

func RawPartitionMerkleTree(epoch int64, globalEpoch bool, partitionId int) (*merkletree.MerkleTree, error) {
	partition, err := store.getPartition(partitionId)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	var lowerEpoch int
	if globalEpoch {
		lowerEpoch = 0
	} else {
		lowerEpoch = int(epoch)
	}
	upperEpoch := int(epoch + 1)

	// Build content list in sorted order of keys
	bucketList := make([]merkletree.Content, partitionBuckets)

	for i := range bucketList {
		bucket := MerkleBucket{hasher: &CustomHash{}, bucketId: int32(i)}
		itemsMap := partition.Items(i, lowerEpoch, upperEpoch)
		for _, v := range itemsMap {
			bucket.AddValue(v)
		}

		bucketList[i] = bucket
	}

	tree, err := merkletree.NewTree(bucketList)
	if err != nil {
		logrus.Debug(err)
		return nil, err
	}
	// merkletreeStore.Add(fmt.Sprintf("%d-%d", partitionEpoch, epoch), tree, 0)

	return tree, nil
}

func CachePartitionMerkleTree(epoch int64, partitionId int) (*merkletree.MerkleTree, error) {
	buckets := bucketsMap[epoch][partitionId]

	contentList := make([]merkletree.Content, len(buckets))

	for i := range contentList {
		contentList[i] = buckets[i]
	}

	tree, err := merkletree.NewTree(contentList)
	if err != nil {
		logrus.Debug(err)
		return nil, err
	}

	return tree, nil
}
