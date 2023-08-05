// sum_test.go
package main

import (
	"log"
	"testing"
)

func Sum(x int, y int) int {
	return x + y
}

func TestSum(t *testing.T) {
	log.Println("TestSum")
	total := Sum(2, 2)
	if total != 4 {
		t.Errorf("Sum was incorrect, got: %d, want: %d.", total, 10)
	}
}

func InitNodeData() {
	nodeData = make(map[string]string)
}

func TestHashRingGetN0(t *testing.T) {
	InitNodeData()
	nodeAns, err := hashRingGetN("Test", 1)
	if err == nil {
		t.Error("No error returned")
	}

	if len(nodeAns) != 0 {
		t.Error("incorrect number of nodes returned")
	}
}

func TestHashRingGetN1(t *testing.T) {
	InitNodeData()
	nodeData["key1"] = "value1"
	nodeData["key2"] = "value2"
	nodeAns, err := hashRingGetN("Test", 1)
	if err != nil {
		t.Error(err)
	}

	if len(nodeAns) != 1 {
		t.Error("incorrect number of nodes returned", len(nodeAns), nodeAns)
	}
}

func TestHashRingGetN2(t *testing.T) {
	InitNodeData()
	nodeData["key1"] = "value1"
	nodeData["key2"] = "value2"
	nodeData["key3"] = "value3"
	nodeData["key4"] = "value4"

	nodeAns, err := hashRingGetN("key4", 2)
	if err != nil {
		t.Error(err)
	}

	if len(nodeAns) != 2 {
		t.Error("incorrect number of nodes returned", len(nodeAns), nodeAns)
	}

}

func TestHashRingGetN4Error(t *testing.T) {
	InitNodeData()
	nodeData["key1"] = "value1"
	nodeData["key2"] = "value2"
	nodeData["key3"] = "value3"

	nodeAns, err := hashRingGetN("Test", 4)
	if err == nil {
		t.Error("Should have returned error")
	}

	if len(nodeAns) != 3 {
		t.Error("incorrect number of nodes returned", len(nodeAns), nodeAns)
	}

	log.Println("RETURNED", nodeAns)
}
