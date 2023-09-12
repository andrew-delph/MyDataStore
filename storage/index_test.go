package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	index := NewIndex("test").
		AddColumn(CreateUnorderedColumn("part1")).
		AddColumn(CreateUnorderedColumn("bucket1")).
		AddColumn(CreateOrderedColumn("1", 4)).
		AddColumn(CreateUnorderedColumn("mykey")).
		Build()
	assert.EqualValues(t, "test_part1_bucket1_0001_mykey", index, "index should be equal")

	index2 := NewIndex("zzz").
		AddColumn(CreateUnorderedColumn("part1")).
		AddColumn(CreateUnorderedColumn("bucket1")).
		AddColumn(CreateOrderedColumn("100", 4)).
		AddColumn(CreateUnorderedColumn("mykey")).
		Build()
	assert.EqualValues(t, "zzz_part1_bucket1_0100_mykey", index2, "index2 should be equal")
}
