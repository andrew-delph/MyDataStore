package hashring

import (
	"fmt"
	"testing"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
)

type TestMember struct {
	name string
	test string
}

func (m TestMember) String() string {
	return m.name
}

func TestHashringRejoin(t *testing.T) {
	// This test proves how AddNode works. it will not override the node.
	c1 := config.GetConfig().Manager
	c1.PartitionCount = 100
	c1.PartitionReplicas = 3
	c1.RingDebounce = 0.1
	c1.Load = 1.25
	hr1 := CreateHashring(c1, nil)
	hr1.AddNode("test1")
	hr1.AddNode("test1")

	hr1.UpdateRing()

	node := hr1.GetMembers(false)[0]

	assert.EqualValues(t, "test1", node.String(), "member.test wrong value")
}

func TestHashringLoad(t *testing.T) {
	c1 := config.GetConfig().Manager
	c1.PartitionCount = 100
	c1.PartitionReplicas = 3
	c1.Load = 1.25
	hr1 := CreateHashring(c1, nil)
	hr1.AddNode("test1")
	hr1.AddNode("test2")
	hr1.AddNode("test3")

	// c2 := config.GetConfig().Manager
	// c2.PartitionCount = 100
	// c2.ReplicaCount = 5
	// c2.Load = 1.25

	c2 := c1

	hr2 := CreateHashring(c2, nil)
	hr2.AddNode("test1")
	hr2.AddNode("test2")

	hr2.AddNode("testx")
	hr2.RemoveNode("testx")

	hr2.AddNode("test3")

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

	loads1 := hr1.currConsistent.LoadDistribution()
	loads2 := hr2.currConsistent.LoadDistribution()

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
		member := TestMember{name: fmt.Sprintf("n%d", i)}
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

	before, _ := c.GetClosestNForPartition(1, n)

	// Add a new member
	mem := TestMember{name: fmt.Sprintf("n%d", m+1)}
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

	after, _ := c.GetClosestNForPartition(1, n)

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

func TestHashringDebcouneUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	reqCh := make(chan interface{}, 10)

	c := config.GetConfig().Manager
	c.PartitionCount = 100
	c.PartitionReplicas = 2
	c.RingDebounce = 0.5
	c.Load = 1.25
	hr := CreateHashring(c, reqCh)

	hr.AddNode("test1")
	hr.AddNode("test2")
	hr.RemoveNode("test2")
	hr.AddNode("test3")

	assert.EqualValues(t, 0, len(hr.GetMembers(false)), "wrong number of members")

	time.Sleep(time.Duration(0.6 * float64(time.Second)))
	assert.EqualValues(t, 2, len(hr.GetMembers(false)), "wrong number of members")
}

func TestHashringTempNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	reqCh := make(chan interface{}, 10)
	go func() {
		for {
			rawEvent := <-reqCh
			event := rawEvent.(RingUpdateTask)
			event.ResCh <- true
		}
	}()
	c := config.GetConfig().Manager
	c.PartitionCount = 100
	c.PartitionReplicas = 2
	c.ReplicaCount = 1
	c.RingDebounce = 0.5
	c.Load = 1.25
	hr := CreateHashring(c, reqCh)
	members := []string{"test1", "test2", "test3"}

	hr.SetRingMembers(members)
	assert.EqualValues(t, 3, len(hr.GetMembers(true)), "wrong number of members")

	changed := hr.SetTempRingMembers(members)
	assert.EqualValues(t, false, changed, "changed is wrong")
	assert.EqualValues(t, 3, len(hr.GetMembers(true)), "wrong number of members")

	temp_members := []string{"test1", "test2", "test3", "test4"}

	changed = hr.SetTempRingMembers(temp_members)
	assert.EqualValues(t, true, changed, "changed is wrong")
	assert.EqualValues(t, 4, len(hr.GetMembers(true)), "wrong number of members")
	assert.EqualValues(t, 3, len(hr.GetMembers(false)), "wrong number of members")
}
