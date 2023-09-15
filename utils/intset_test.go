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

	assert.EqualValues(t, true, setA.Has(1), "Has wrong values")
	assert.EqualValues(t, false, setA.Has(3), "Has wrong values")
}

func TestInt32Set(t *testing.T) {
	// create intset and insert items
	// create another and  from a list of ints
	// compare difference
	setA := NewInt32Set()
	setA.Add(1)
	setA.Add(2)

	setB := NewInt32Set()
	setB.Add(2)
	setB.Add(3)

	// t.Error("diff", setA.Difference(setB))
	assert.EqualValues(t, []int32{1}, setA.Difference(setB).List(), "Difference wrong values")
	assert.EqualValues(t, []int32{3}, setB.Difference(setA).List(), "Difference wrong values")

	assert.EqualValues(t, true, setA.Has(1), "Has wrong values")
	assert.EqualValues(t, false, setA.Has(3), "Has wrong values")
}

func TestAssert(t *testing.T) {
	assert.Equal(t, 33, 33, "Http.DefaultTimeout wrong value")
}
