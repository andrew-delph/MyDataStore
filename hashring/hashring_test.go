package hashring

import (
	"fmt"
	"testing"

	"github.com/buraksezer/consistent"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
)

type TestMember string

func (m TestMember) String() string {
	return string(m)
}

func TestHashringLoad(t *testing.T) {
	c1 := config.GetConfig().Manager
	c1.PartitionCount = 100
	c1.PartitionReplicas = 3
	c1.Load = 1.25
	hr1 := CreateHashring(c1)
	hr1.AddNode(TestMember("test1"))
	hr1.AddNode(TestMember("test2"))
	hr1.AddNode(TestMember("test3"))

	// c2 := config.GetConfig().Manager
	// c2.PartitionCount = 100
	// c2.ReplicaCount = 5
	// c2.Load = 1.25

	c2 := c1

	hr2 := CreateHashring(c2)
	hr2.AddNode(TestMember("test1"))
	hr2.AddNode(TestMember("test2"))

	hr2.AddNode(TestMember("testx"))
	hr2.RemoveNode("testx")

	hr2.AddNode(TestMember("test3"))

	hr1_1, _ := hr1.GetMemberPartions("test1")
	hr2_1, _ := hr2.GetMemberPartions("test1")
	assert.EqualValues(t, hr1_1, hr2_1, "has equal partitions")

	hr1_2, _ := hr1.GetMemberPartions("test2")
	hr2_2, _ := hr2.GetMemberPartions("test2")
	assert.EqualValues(t, hr1_2, hr2_2, "has equal partitions")

	// for i := 0; i < 100; i++ {
	// 	key := "testkey"
	// 	logrus.Warn(key)

	// }
	// t.Error("!")

	loads1 := hr1.consistent.LoadDistribution()
	loads2 := hr2.consistent.LoadDistribution()

	// logrus.Warn("loads1")
	// logrus.Warn(loads1)

	// logrus.Warn("loads2")
	// logrus.Warn(loads2)

	assert.EqualValues(t, loads1, loads2, "equal loads")
}

func TestHashringRelocation(t *testing.T) {
	m := 30
	p := 200
	n := 1
	l := 1.5

	total := 0
	for i := 3; i < m; i++ {
		total = total + diffTest(i, p, n, l)
	}

	logrus.Info("total= ", total)
}

func diffTest(m, p, n int, l float64) int {
	cfg := consistent.Config{
		PartitionCount:    p,
		ReplicationFactor: n,
		Load:              l,
		Hasher:            hasher{},
	}
	members := []consistent.Member{}
	for i := 0; i < m; i++ {
		member := TestMember(fmt.Sprintf("n%d", i))
		members = append(members, member)
	}
	// Modify PartitionCount, ReplicationFactor and Load to increase or decrease
	// relocation ratio.

	c := consistent.New(members, cfg)

	// Store current layout of partitions
	owners := make(map[int]string)
	for partID := 0; partID < cfg.PartitionCount; partID++ {
		owners[partID] = c.GetPartitionOwner(partID).String()
	}

	before, _ := c.GetClosestN([]byte("key"), n)

	// Add a new member
	mem := TestMember(fmt.Sprintf("n%d", m+1))
	c.Add(mem)

	// Get the new layout and compare with the previous
	var changed int
	for partID, member := range owners {
		owner := c.GetPartitionOwner(partID)
		if member != owner.String() {
			changed++
			// fmt.Printf("partID: %3d moved to %s from %s\n", partID, owner.String(), member)
		}
	}

	after, _ := c.GetClosestN([]byte("key"), n)

	diff := findDifferentCount(before, after)
	logrus.Infof("diff: %v                            m:%d", diff, m)

	// logrus.Infof("%d%% of the partitions are relocated", (100*changed)/cfg.PartitionCount)
	return diff
}

func findDifferentCount(slice1, slice2 []consistent.Member) int {
	differentCount := 0

	// Create a map to track unique elements in slice1
	uniqueElements := make(map[consistent.Member]struct{})

	// Add all elements from slice1 to the map
	for _, m := range slice1 {
		uniqueElements[m] = struct{}{}
	}

	// Check each element in slice2
	// If it's in the map, mark it as found and delete it from the map
	for _, m := range slice2 {
		if _, found := uniqueElements[m]; found {
			delete(uniqueElements, m)
		} else {
			differentCount++
		}
	}

	// Count the remaining unique elements in slice1
	differentCount += len(uniqueElements)

	return differentCount
}
