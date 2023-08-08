package main

import (
	"fmt"
	"testing"

	"github.com/serialx/hashring"
	"github.com/sirupsen/logrus"
)

var totalKeys = 1000

func TestHashRing1(t *testing.T) {
	// using "github.com/serialx/hashring"
	// Create an empty hash ring
	hashRing := hashring.New([]string{})

	// Add two nodes to the hash ring
	hashRing = hashRing.AddNode(randomString(7))
	hashRing = hashRing.AddNode(randomString(7))
	hashRing = hashRing.AddNode(randomString(7))

	// Print the nodes in the hash ring
	logrus.Println(hashRing.GetNode("test"))
	logrus.Println("size", hashRing.Size())

	nodeCount := make(map[string]int)

	for i := 1; i <= totalKeys; i++ {
		// nodeName, _ := hashRing.GetNode(randomString(7))
		nodeName, _ := hashRing.GetNode(fmt.Sprintf("key%d", i))
		nodeCount[nodeName]++
	}

	logrus.Info(nodeCount)

	for node, count := range nodeCount {
		percentage := float64(count) / float64(totalKeys) * 100
		logrus.Infof("Node: %s, Keys: %d, Percentage: %.2f%%", node, count, percentage)
	}
}

func TestHashRing2(t *testing.T) {
	// using hashring.go

	c1 := GetHashRing()
	c2 := GetHashRing()

	// Add some members to the consistent hash table.
	// Add function calculates average load and distributes partitions over members
	node1 := randomString(7)
	AddNode(c1, node1)
	AddNode(c2, node1)

	node2 := randomString(7)

	AddNode(c1, node2)
	AddNode(c2, node2)

	node3 := randomString(7)
	AddNode(c1, node3)
	AddNode(c2, node3)

	// calculates partition id for the given key
	// partID := hash(key) % partitionCount
	// the partitions are already distributed among members by Add function.

	// Prints node2.olric.com

	nodeCount1 := make(map[string]int)
	nodeCount2 := make(map[string]int)

	for i := 1; i <= totalKeys; i++ {
		// nodeName, _ := hashRing.GetNode(randomString(7))
		// key := []byte(fmt.Sprintf("key%d", i))
		key := []byte(randomString(7))
		nodeName1 := c1.LocateKey(key)
		nodeCount1[nodeName1.String()]++
		nodeName2 := c2.LocateKey(key)
		nodeCount2[nodeName2.String()]++
	}

	logrus.Info("nodeCount1", nodeCount1)
	logrus.Info("nodeCount2", nodeCount2)

	for node, count := range nodeCount1 {
		percentage := float64(count) / float64(totalKeys) * 100
		logrus.Infof("Node: %s, Keys: %d, Percentage: %.2f%%", node, count, percentage)
	}
	logrus.Info(("HERE"))

	logrus.Info(GetMembers(c1))

	removeMember := node2

	logrus.Info("removeMember: ", removeMember)

	RemoveNode(c1, removeMember)

	logrus.Info(GetMembers(c1))
}
