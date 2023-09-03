package main

import (
	"fmt"
	"testing"

	"github.com/hashicorp/memberlist"
	"github.com/serialx/hashring"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
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
	defer func() {
		if r := recover(); r != nil {
			t.Error("Recovered from panic:", r)
		}
	}()
	config := config.GetConfig(true)
	c1 := GetHashRing(config.Manager)
	c2 := GetHashRing(config.Manager)

	node1 := &memberlist.Node{Name: "node1"}

	c1.AddNode(node1)
	c2.AddNode(node1)

	node2 := &memberlist.Node{Name: "node2"}

	c1.AddNode(node2)
	c2.AddNode(node2)

	node3 := &memberlist.Node{Name: "node3"}
	c1.AddNode(node3)
	c2.AddNode(node3)

	nodeCount1 := make(map[string]int)
	nodeCount2 := make(map[string]int)

	for i := 1; i <= totalKeys; i++ {
		// nodeName, _ := hashRing.GetNode(randomString(7))
		// key := []byte(fmt.Sprintf("key%d", i))
		key := []byte(randomString(7))
		nodeName1 := c1.Consistent.LocateKey(key)
		nodeCount1[nodeName1.String()]++
		nodeName2 := c2.Consistent.LocateKey(key)
		nodeCount2[nodeName2.String()]++
	}

	logrus.Info("nodeCount1", nodeCount1)
	logrus.Info("nodeCount2", nodeCount2)
	assert.EqualValues(t, len(c1.GetMembers()), 3, "c1 should have len 3")
	assert.EqualValues(t, len(c1.GetMembers()), len(c2.GetMembers()), "nodeCount1 and nodeCount2 should be equal")

	for node, count := range nodeCount1 {
		percentage := float64(count) / float64(totalKeys) * 100
		logrus.Infof("Node: %s, Keys: %d, Percentage: %.2f%%", node, count, percentage)
	}

	logrus.Info(c1.GetMembers())

	removeMember := node2

	logrus.Info("removeMember: ", removeMember)

	c1.RemoveNode(removeMember)

	assert.EqualValues(t, len(c1.GetMembers())+1, len(c2.GetMembers()), "c1 should have 1 less than c2")

	logrus.Info(c1.GetMembers())

	closestN, err := c1.GetClosestN("test", 1)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, 1, len(closestN), "1 node should be returned.")

	next := closestN[0]

	assert.EqualValues(t, "node1", next.name, "should be the correct name")
}

func TestPartitions(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Error("Recovered from panic:", r)
		}
	}()
	config := config.GetConfig(true)
	ring := GetHashRing(config.Manager)

	for i := 0; i < 8; i++ {
		ring.AddNode(&memberlist.Node{Name: fmt.Sprintf("node%d", i)})
	}

	// Store current layout of partitions
	owners := make(map[int]string)
	for partID := 0; partID < config.Manager.PartitionCount; partID++ {
		owners[partID] = ring.Consistent.GetPartitionOwner(partID).String()
	}

	ring.AddNode(&memberlist.Node{Name: fmt.Sprintf("node%d", 9)})

	// Get the new layout and compare with the previous
	var changed int
	for partID, member := range owners {
		owner := ring.Consistent.GetPartitionOwner(partID)
		if member != owner.String() {
			changed++
			fmt.Printf("partID: %3d moved to %s from %s\n", partID, owner.String(), member)
		}
	}
	fmt.Printf("\n%d%% of the partitions are relocated\n", (100*changed)/config.Manager.PartitionCount)
}
