package main

import (
	"container/list"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cbergoon/merkletree"
	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
)

func TestGoCacheStoreMerkleTree(t *testing.T) {
	t.Error("todo fix.")
	return
	config := config.GetConfig()
	config.Manager.Hostname = randomString(5)

	var err error
	store, err = NewLevelDbStore(config.Manager)
	defer store.Close()

	for i := 0; i < NumTestValues; i++ {
		store.SetValue(testValue(fmt.Sprintf("keyz%d", i), fmt.Sprintf("value%d", i), 1))
	}

	startTime := time.Now()

	tree1, _, err := RawPartitionMerkleTree(1, true, 1)
	if err != nil {
		t.Error(err)
	}

	_, err = tree1.VerifyTree()
	if err != nil {
		t.Error(err)
	}

	tree2, _, err := RawPartitionMerkleTree(1, true, 1)
	if err != nil {
		t.Error(err)
	}

	err = tree1.RebuildTree()
	if err != nil {
		t.Error(err)
	}

	err = tree2.RebuildTree()
	if err != nil {
		t.Error(err)
	}

	assert.EqualValues(t, tree1.Root.Hash, tree2.Root.Hash, "Tree hashes don't match")

	elapsedTime := time.Since(startTime).Seconds()

	fmt.Printf("GoCache Elapsed Time: %.2f seconds\n", elapsedTime)
}

func TestLevelDbStoreRawMerkleTreeTODOFIX(t *testing.T) {
	t.Error("todo fix.")
	// config := config.GetConfig()
	// config.Hostname = randomString(5)

	// var err error
	// store, err = NewLevelDbStore(config.Manager)
	// if err != nil {
	// 	t.Error(fmt.Sprintf("NewLevelDbStore: %v", err))
	// }
	// defer store.Close()
	// extraKey := "Extra"
	// extraPartition := FindPartitionID(events.consistent, extraKey)
	// setEpoch := 5

	// for i := 0; i < 100; i++ {
	// 	err := store.SetValue(testValue(fmt.Sprintf("keyz%d", i), fmt.Sprintf("value%d", i), setEpoch))
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// }

	// startTime := time.Now()

	// tree1, buckets1, err := RawPartitionMerkleTree(int64(setEpoch), false, extraPartition)
	// if err != nil {
	// 	t.Error(err)
	// }

	// tree2, _, err := RawPartitionMerkleTree(int64(setEpoch), false, extraPartition)
	// if err != nil {
	// 	t.Error(err)
	// }
	// assert.EqualValues(t, tree1.Root.Hash, tree2.Root.Hash, "Tree hashes don't match")

	// // _, err = tree1.VerifyTree()
	// // if err != nil {
	// // 	t.Error(err)
	// // }

	// err = store.SetValue(testValue(extraKey, "Extra2", 1))
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// tree3, _, err := RawPartitionMerkleTree(int64(setEpoch), true, extraPartition)
	// if err != nil {
	// 	t.Error(err)
	// }

	// assert.NotEqual(t, tree1.Root.Hash, tree3.Root.Hash, "Tree hashes match")
	// assert.NotEqual(t, tree2.Root.Hash, tree3.Root.Hash, "Tree hashes match")

	// diff1 := DifferentMerkleTreeBuckets(tree1, tree3)
	// diff2 := DifferentMerkleTreeBuckets(tree3, tree1)

	// // serialize the tree and unserialize...

	// partitionEpochObject, err := MerkleTreeToPartitionEpochObject(tree1, buckets1, int64(setEpoch), extraPartition)
	// if err != nil {
	// 	t.Error(err)
	// }
	// serializeTree1, err := PartitionEpochObjectToMerkleTree(partitionEpochObject)
	// if err != nil {
	// 	t.Error(err)
	// }

	// diff3 := DifferentMerkleTreeBuckets(tree3, serializeTree1)
	// diff4 := DifferentMerkleTreeBuckets(serializeTree1, tree3)

	// assert.EqualValues(t, diff1, diff2, "diffs dont match")
	// assert.EqualValues(t, diff2, diff3, "diffs dont match")
	// assert.EqualValues(t, diff3, diff4, "diffs dont match")

	// // Test global
	// tree4, _, err := RawPartitionMerkleTree(2, true, extraPartition)
	// if err != nil {
	// 	t.Error(err)
	// }

	// tree5, _, err := RawPartitionMerkleTree(1, false, extraPartition)
	// if err != nil {
	// 	t.Error(err)
	// }

	// assert.EqualValues(t, tree4.Root.Hash, tree5.Root.Hash, "Tree hashes don't match")

	// tree6, _, err := RawPartitionMerkleTree(2, false, extraPartition)
	// if err != nil {
	// 	t.Error(err)
	// }

	// tree7, _, err := RawPartitionMerkleTree(1, false, extraPartition)
	// if err != nil {
	// 	t.Error(err)
	// }

	// assert.NotEqual(t, tree6.Root.Hash, tree7.Root.Hash, "Tree hashes don't match")

	// // tree6, err := PartitionMerkleTree(int64(setEpoch), true, extraPartition)
	// // if err != nil {
	// // 	t.Error(err)
	// // }

	// elapsedTime := time.Since(startTime).Seconds()

	// fmt.Printf("LevelDb Elapsed Time: %.2f seconds\n", elapsedTime)
}

func BFS(root *merkletree.Node) []*merkletree.Node {
	if root == nil {
		return nil
	}

	queue := list.New()
	queue.PushBack(root)

	var result []*merkletree.Node

	for queue.Len() > 0 {
		current := queue.Remove(queue.Front()).(*merkletree.Node)
		result = append(result, current)

		if current.Left != nil {
			queue.PushBack(current.Left)
		}

		if current.Right != nil {
			queue.PushBack(current.Left)
		}
	}

	return result
}

// TestContent implements the Content interface provided by merkletree and represents the content stored in the tree.
type TestContent struct {
	x string
}

// CalculateHash hashes the values of a TestContent
func (t TestContent) CalculateHash() ([]byte, error) {
	h := sha256.New()
	if _, err := h.Write([]byte(t.x)); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// Equals tests for equality of two Contents
func (t TestContent) Equals(other merkletree.Content) (bool, error) {
	otherTC, ok := other.(TestContent)
	if !ok {
		return false, errors.New("value is not of type TestContent")
	}
	return t.x == otherTC.x, nil
}

func TestCustomHash(t *testing.T) {
	customHash := &CustomHash{}

	for i := 0; i < 100; i++ {
		customHash.Add([]byte(randomString(5)))
	}

	customHash.Add([]byte("world"))
	hash1 := customHash.Hash()
	fmt.Printf("Hash after adding 'world': %d\n", hash1)

	customHash.Add([]byte("hello"))
	hash2 := customHash.Hash()
	fmt.Printf("Hash after adding 'hello': %d\n", hash2)

	customHash.Remove([]byte("hello"))
	hash3 := customHash.Hash()
	fmt.Printf("Hash after removing 'hello': %d\n", hash3)

	assert.Equal(t, hash1, hash3, "The hash values should be equal")
}
