package main

import (
	"fmt"
	"net"
	"testing"

	"github.com/hashicorp/memberlist"
	"github.com/serialx/hashring"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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

	c1 := GetHashRing()
	c2 := GetHashRing()

	node1 := &memberlist.Node{Name: "node1", Addr: net.ParseIP("192.168.1.1")}

	AddNode(c1, node1)
	AddNode(c2, node1)

	node2 := &memberlist.Node{Name: "node2", Addr: net.ParseIP("192.168.1.2")}

	AddNode(c1, node2)
	AddNode(c2, node2)

	node3 := &memberlist.Node{Name: "node3", Addr: net.ParseIP("192.168.1.3")}
	AddNode(c1, node3)
	AddNode(c2, node3)

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
	assert.EqualValues(t, len(GetMembers(c1)), 3, "c1 should have len 3")
	assert.EqualValues(t, len(GetMembers(c1)), len(GetMembers(c2)), "nodeCount1 and nodeCount2 should be equal")

	for node, count := range nodeCount1 {
		percentage := float64(count) / float64(totalKeys) * 100
		logrus.Infof("Node: %s, Keys: %d, Percentage: %.2f%%", node, count, percentage)
	}

	logrus.Info(GetMembers(c1))

	removeMember := node2

	logrus.Info("removeMember: ", removeMember)

	RemoveNode(c1, removeMember)

	assert.EqualValues(t, len(GetMembers(c1))+1, len(GetMembers(c2)), "c1 should have 1 less than c2")

	logrus.Info(GetMembers(c1))

	closestN, err := GetClosestN(c1, "test", 1)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, 1, len(closestN), "1 node should be returned.")

	next := closestN[0]

	assert.EqualValues(t, "node1", next.name, "should be the correct name")
	assert.EqualValues(t, "192.168.1.1", next.addr, "should be the correct addr")
}

func TestPartitions(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Error("Recovered from panic:", r)
		}
	}()
	c := GetHashRing()

	for i := 0; i < 8; i++ {
		AddNode(c, &memberlist.Node{Name: fmt.Sprintf("node%d", i), Addr: net.ParseIP(fmt.Sprintf("192.168.1.%d", i))})
	}

	// Store current layout of partitions
	owners := make(map[int]string)
	for partID := 0; partID < hashRingConf.PartitionCount; partID++ {
		owners[partID] = c.GetPartitionOwner(partID).String()
	}

	AddNode(c, &memberlist.Node{Name: fmt.Sprintf("node%d", 9), Addr: net.ParseIP(fmt.Sprintf("192.168.1.%d", 9))})

	// Get the new layout and compare with the previous
	var changed int
	for partID, member := range owners {
		owner := c.GetPartitionOwner(partID)
		if member != owner.String() {
			changed++
			fmt.Printf("partID: %3d moved to %s from %s\n", partID, owner.String(), member)
		}
	}
	fmt.Printf("\n%d%% of the partitions are relocated\n", (100*changed)/hashRingConf.PartitionCount)
}
