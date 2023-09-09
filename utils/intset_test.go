package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntSet(t *testing.T) {
	// create intset and insert items
	// create another and  from a list of ints
	// compare difference
	setA := NewIntSet()
	setA.Add(1)
	setA.Add(2)

	setB := NewIntSet()
	setB.Add(2)
	setB.Add(3)

	// t.Error("diff", setA.Difference(setB))
	assert.EqualValues(t, []int{1}, setA.Difference(setB).List(), "Difference wrong values")
	assert.EqualValues(t, []int{3}, setB.Difference(setA).List(), "Difference wrong values")
}

func TestAssert(t *testing.T) {
	assert.Equal(t, 33, 33, "Http.DefaultTimeout wrong value")
}
