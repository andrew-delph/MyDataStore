package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/rpc"
)

func TestMerkleTree(t *testing.T) {
	var err error
	// if testing.Short() {
	// 	t.Skip("skipping test in short mode.")
	// }
	manager := createMockManager(t)

	writeValuesNum := 10

	// write to epoch 1
	for i := 0; i < writeValuesNum; i++ {
		k := fmt.Sprintf("key%d", i)
		v := fmt.Sprintf("val%d", i)
		setVal := &rpc.RpcValue{Key: k, Value: v, Epoch: 1}
		err = manager.SetValue(setVal)
		if err != nil {
			t.Error(err)
		}
		getVal, err := manager.GetValue(k)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, v, getVal.Value, "get value is wrong")
	}

	// write to epoch 2
	for i := 0; i < writeValuesNum; i++ {
		k := fmt.Sprintf("keyz%d", i)
		v := fmt.Sprintf("valz%d", i)
		setVal := &rpc.RpcValue{Key: k, Value: v, Epoch: 2}
		err = manager.SetValue(setVal)
		if err != nil {
			t.Error(err)
		}
		getVal, err := manager.GetValue(k)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, v, getVal.Value, "get value is wrong")
	}

	// check iterator for both...

	tree1, err := manager.RawPartitionMerkleTree(0, 0, 3)
	if err != nil {
		t.Error(err)
	}
	tree2, err := manager.RawPartitionMerkleTree(0, 1, 3)
	if err != nil {
		t.Error(err)
	}

	assert.EqualValues(t, tree1.MerkleRoot(), tree2.MerkleRoot(), "hash should be the same")

	tree3, err := manager.RawPartitionMerkleTree(0, 2, 3)
	if err != nil {
		t.Error(err)
	}

	assert.NotEqualValues(t, tree1.MerkleRoot(), tree3.MerkleRoot(), "hash should be the same")
}
