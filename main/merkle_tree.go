package main

import (
	"bytes"
	"reflect"

	"github.com/cbergoon/merkletree"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/rpc"
	"github.com/andrew-delph/my-key-store/utils"
)

func testMT() {
	logrus.Info("hi")
}

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

type MerkleBucket struct {
	bucketId int32
	hasher   *CustomHash
}

func (bucket MerkleBucket) CalculateHash() ([]byte, error) {
	hash, err := utils.EncodeInt64ToBytes(bucket.hasher.Hash())
	if err != nil {
		logrus.Errorf("MerkleBucket CalculateHash err = %v", err)
		return nil, err
	}
	return hash, nil
}

func (bucket MerkleBucket) AddItem(bytes []byte) error {
	bucket.hasher.Add(bytes)
	return nil
}

func (content MerkleBucket) Equals(other merkletree.Content) (bool, error) {
	otherTC, ok := other.(MerkleBucket)
	if !ok {
		return false, errors.New("value is not of type MerkleContent")
	}
	return content.hasher.Hash() == otherTC.hasher.Hash(), nil
}

func (manager *Manager) RawPartitionMerkleTree(partitionId int, lowerEpoch, upperEpoch int64) (*merkletree.MerkleTree, error) {
	// Build content list in sorted order of keys
	bucketList := make([]merkletree.Content, manager.config.Manager.PartitionBuckets)

	count := 0
	for i := 0; i < manager.config.Manager.PartitionBuckets; i++ {
		bucket := MerkleBucket{hasher: &CustomHash{}, bucketId: int32(i)}
		index1, err := BuildEpochIndex(partitionId, uint64(i), lowerEpoch, "")
		if err != nil {
			logrus.Fatal(err)
		}
		index2, err := BuildEpochIndex(partitionId, uint64(i), upperEpoch, "")
		if err != nil {
			logrus.Fatal(err)
		}
		it := manager.db.NewIterator([]byte(index1), []byte(index2))
		for !it.IsDone() {
			bucket.AddItem(it.Value())
			count++
			it.Next()
		}
		it.Release()

		bucketList[i] = &bucket
	}

	return merkletree.NewTree(bucketList)
}

func MerkleTreeToPartitionEpochObject(tree *merkletree.MerkleTree, partitionId int, lowerEpoch, upperEpoch int64) (*rpc.RpcEpochTreeObject, error) {
	bucketHashes := make([][]byte, 0)
	for _, leaf := range tree.Leafs {
		bucket, ok := leaf.C.(*MerkleBucket)
		if !ok {
			return nil, errors.Errorf("failed to decode MerkleBucket. type = %s", reflect.TypeOf(leaf))
		}
		hash, err := bucket.CalculateHash()
		if err != nil {
			return nil, errors.Wrap(err, "MerkleTreeToPartitionEpochObject")
		}
		bucketHashes = append(bucketHashes, hash)
	}

	epochTreeObject := &rpc.RpcEpochTreeObject{LowerEpoch: lowerEpoch, UpperEpoch: upperEpoch, Partition: int32(partitionId), Buckets: bucketHashes}
	return epochTreeObject, nil
}

func EpochTreeObjectToMerkleTree(partitionEpochObject *rpc.RpcEpochTreeObject) (*merkletree.MerkleTree, error) {
	contentList := make([]merkletree.Content, 0)
	for i, bucketHash := range partitionEpochObject.Buckets {
		hashInt64, err := utils.DecodeBytesToInt64(bucketHash)
		if err != nil {
			return nil, err
		}
		contentList = append(contentList, &MerkleBucket{hasher: &CustomHash{value: hashInt64}, bucketId: int32(i)})
	}
	return merkletree.NewTree(contentList)
}

func DifferentMerkleTreeBuckets(tree1 *merkletree.MerkleTree, tree2 *merkletree.MerkleTree) ([]int32, error) {
	return DifferentMerkleTreeBucketsDFS(tree1.Root, tree2.Root)
}

func DifferentMerkleTreeBucketsDFS(node1 *merkletree.Node, node2 *merkletree.Node) ([]int32, error) {
	differences := []int32{}

	if node1 == nil && node2 == nil {
		return differences, nil
	} else if node1 == nil || node2 == nil {
		return nil, errors.Errorf("a merkletree.Node is nil. node1=%s node2=%s", node1, node2)
	}

	if !bytes.Equal(node1.Hash, node2.Hash) {
		if node1.Left == nil && node1.Right == nil {
			var bucketId1 int32
			switch bucket1 := node1.C.(type) {
			case *MerkleBucket:
				bucketId1 = bucket1.bucketId
			default:
				return nil, errors.Errorf("failed to decode MerkleBucket. type = %s", reflect.TypeOf(bucket1))
			}

			var bucketId2 int32
			switch bucket2 := node2.C.(type) {
			case *MerkleBucket:
				bucketId2 = bucket2.bucketId
			default:
				return nil, errors.Errorf("failed to decode MerkleBucket. type = %s", reflect.TypeOf(bucket2))
			}

			if bucketId1 != bucketId2 {
				return nil, errors.Errorf("bucketIds dont match. bucketId1 %v bucketId2%v", bucketId1, bucketId2)
			}

			differences = append(differences, bucketId1)
		} else {
			// Recurse into child nodes
			diff1, err := DifferentMerkleTreeBucketsDFS(node1.Left, node2.Left)
			if err != nil {
				return nil, err
			}
			differences = append(differences, diff1...)
			diff2, err := DifferentMerkleTreeBucketsDFS(node1.Right, node2.Right)
			if err != nil {
				return nil, err
			}
			differences = append(differences, diff2...)
		}
	}

	return differences, nil
}
