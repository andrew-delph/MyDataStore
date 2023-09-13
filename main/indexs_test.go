package main

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexManager(t *testing.T) {
	parition := 1
	bucket := uint64(2)
	epoch := int64(3)
	key := "zz"
	index1, err := BuildEpochIndex(parition, bucket, epoch, key)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "epoch_1_2_0003_zz", index1, "equal index")
	index1Parse, err := ParseEpochIndex(index1)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "epoch", index1Parse["name"], "parsed correct")
	assert.Equal(t, strconv.FormatInt(int64(parition), 10), index1Parse["parition"], "parsed correct")
	assert.Equal(t, strconv.FormatInt(int64(bucket), 10), index1Parse["bucket"], "parsed correct")
	assert.Equal(t, strconv.FormatInt(int64(epoch), 10), index1Parse["epoch"], "parsed correct")
	assert.Equal(t, key, index1Parse["key"], "parsed correct")
}
