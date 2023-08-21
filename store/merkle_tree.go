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

var GLOBAL_BUCKET_ERROR = errors.New("Could not update global bucket.")

var NOT_EXIST_BUCKET_ERROR = errors.New("Bucket does not exist.")

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

func (h *CustomHash) Merge(other *CustomHash) {
	h.value += other.value
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
	hash, err := EncodeInt64ToBytes(bucket.hasher.Hash())
	if err != nil {
		logrus.Errorf("MerkleBucket CalculateHash err = %v", err)
		return nil, err
	}
	return hash, nil
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

func (bucket MerkleBucket) MergeBucket(other *MerkleBucket) error {
	bucket.hasher.Merge(other.hasher)
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
	} else {
		err := globalBucket[partitionId][bucket].AddValue(value)
		if err != nil {
			return GLOBAL_BUCKET_ERROR
		}
	}
	currBucket, err := GetBucket(epoch, partitionId, bucket)
	if err != nil {
		logrus.Warnf("currBucket.AddValue GetBucket err = %v", err)
		return NOT_EXIST_BUCKET_ERROR
	}
	err = currBucket.AddValue(value)
	if err != nil {
		logrus.Warnf("currBucket.AddValue err = %v", err)
		return err
	}
	return nil
}

func RemoveBucket(epoch int64, partitionId, bucket int, value *pb.Value) error {
	if epoch > currGlobalBucketEpoch {
		//
	} else {
		err := globalBucket[partitionId][bucket].RemoveValue(value)
		if err != nil {
			return GLOBAL_BUCKET_ERROR
		}
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

func GlobalMergeBuckets(epoch int64) error {
	mergeBucketHolder, ok := bucketsMap[epoch]
	if !ok {
		for partitionId := 0; partitionId < partitionCount; partitionId++ {
			_, contentList, err := RawPartitionMerkleTree(epoch, true, partitionId)
			if err != nil {
				logrus.Error("Could not create global MerkleTree partitionId = %d err = %v", partitionId, err)
				return err
			}
			globalBucket[partitionId] = contentList
		}
	}
	for partitionId := 0; partitionId < partitionCount; partitionId++ {

		mergeBuckets := mergeBucketHolder[partitionId]
		globalBuckets := globalBucket[partitionId]

		for i := range mergeBuckets {
			mergeBucket := mergeBuckets[i]
			globalBucket := globalBuckets[i]
			globalBucket.MergeBucket(mergeBucket)
		}

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

func UpdateGlobalBucket(newEpoch int64) error {
	logrus.Debugf("UpdateGlobalBucket newEpoch = %d", newEpoch)
	bucketsMap[newEpoch] = NewBucketsHolder()
	for len(bucketsMap) > bucketEpochLag {
		minBucket := int64(math.MaxInt64)
		for bucket := range bucketsMap {
			minBucket = min(minBucket, bucket)
			min(minBucket, bucket)
		}
		logrus.Debugf("Delete Buckets on Epoch = %v", minBucket)
		delete(bucketsMap, minBucket)
	}
	err := GlobalMergeBuckets(newEpoch - 1)
	if err != nil {
		return err
	}
	currGlobalBucketEpoch = newEpoch - 1
	return err
}

func NewBucketsHolder() map[int][]*MerkleBucket {
	bucketHolder := make(map[int][]*MerkleBucket, partitionCount)
	for partitionId := 0; partitionId < partitionCount; partitionId++ {
		bucketHolder[partitionId] = make([]*MerkleBucket, partitionBuckets)
		for bucketId := 0; bucketId < partitionBuckets; bucketId++ {
			bucketHolder[partitionId][bucketId] = &MerkleBucket{hasher: &CustomHash{}, bucketId: int32(bucketId)}
		}
	}
	return bucketHolder
}

func RawPartitionMerkleTree(epoch int64, globalEpoch bool, partitionId int) (*merkletree.MerkleTree, []*MerkleBucket, error) {
	partition, err := store.getPartition(partitionId)
	if err != nil {
		logrus.Error(err)
		return nil, nil, err
	}
	var lowerEpoch int
	if globalEpoch {
		lowerEpoch = 0
	} else {
		lowerEpoch = int(epoch)
	}
	upperEpoch := int(epoch + 1)

	// Build content list in sorted order of keys
	bucketList := make([]*MerkleBucket, partitionBuckets)

	for i := range bucketList {
		bucket := MerkleBucket{hasher: &CustomHash{}, bucketId: int32(i)}
		itemsMap := partition.Items(i, lowerEpoch, upperEpoch)
		for _, v := range itemsMap {
			bucket.AddValue(v)
		}

		bucketList[i] = &bucket
	}

	contentList := make([]merkletree.Content, partitionBuckets)
	for i := range bucketList {
		contentList[i] = bucketList[i]
	}

	tree, err := merkletree.NewTree(contentList)
	if err != nil {
		logrus.Debug(err)
		return nil, nil, err
	}
	// merkletreeStore.Add(fmt.Sprintf("%d-%d", partitionEpoch, epoch), tree, 0)

	return tree, bucketList, nil
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

func GlobalPartitionMerkleTree(partitionId int) (*merkletree.MerkleTree, error) {
	buckets, ok := globalBucket[partitionId]
	if !ok {
		return nil, fmt.Errorf("GlobalPartitionMerkleTreeglobalBucket[partitionId] not found")
	}

	contentList := make([]merkletree.Content, len(buckets))

	for i := range contentList {
		contentList[i] = buckets[i]
	}

	tree, err := merkletree.NewTree(contentList)
	if err != nil {
		logrus.Debug(err)
		return nil, fmt.Errorf("GlobalPartitionMerkleTree err = %v", err)
	}

	return tree, nil
}
