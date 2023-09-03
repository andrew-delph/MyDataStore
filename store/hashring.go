package main

import (
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/hashicorp/memberlist"
)

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type HashRingMember struct {
	name string
}

func (m HashRingMember) String() string {
	return string(m.name)
}

type HashRing struct {
	Config     consistent.Config
	Consistent *consistent.Consistent
}

func GetHashRing(managerConfig ManagerConfig) *HashRing {
	Config := consistent.Config{
		PartitionCount:    managerConfig.PartitionCount,
		ReplicationFactor: 13,
		Load:              1.2,
		Hasher:            hasher{},
	}
	Consistent := consistent.New(nil, Config)
	return &HashRing{Config: Config, Consistent: Consistent}
}

func (ring HashRing) AddNode(node *memberlist.Node) {
	member := HashRingMember{name: node.Name}
	ring.Consistent.Add(member)
}

func (ring HashRing) RemoveNode(node *memberlist.Node) {
	ring.Consistent.Remove(node.Name)
}

func (ring HashRing) FindPartitionID(key string) int {
	keyBytes := []byte(key)
	return ring.Consistent.FindPartitionID(keyBytes)
}

func (ring HashRing) GetMemberPartions(member string) ([]int, error) {
	var belongsTo []int
	for partID := 0; partID < ring.Config.PartitionCount; partID++ {
		members, err := ring.Consistent.GetClosestNForPartition(partID, ring.Config.PartitionCount)
		if err != nil {
			return nil, err
		}
		partitionMembers := ConvertHashRingMemberArray(members)
		for _, partitionMember := range partitionMembers {
			if partitionMember.String() == member {
				belongsTo = append(belongsTo, partID)
			}
		}
	}
	return belongsTo, nil
}

func (ring HashRing) GetMembers() []HashRingMember {
	return ConvertHashRingMemberArray(ring.Consistent.GetMembers())
}

func (ring HashRing) GetClosestN(key string, count int) ([]HashRingMember, error) {
	keyBytes := []byte(key)

	members, err := ring.Consistent.GetClosestN(keyBytes, count)
	if err != nil {
		return nil, err
	}

	return ConvertHashRingMemberArray(members), nil
}

func (ring HashRing) GetClosestNForPartition(partitionId, count int) ([]HashRingMember, error) {
	members, err := ring.Consistent.GetClosestNForPartition(partitionId, count)
	if err != nil {
		return nil, err
	}

	return ConvertHashRingMemberArray(members), nil
}

func ConvertHashRingMemberArray(members []consistent.Member) []HashRingMember {
	var hashRingMembers []HashRingMember
	for _, member := range members {
		hashRingMembers = append(hashRingMembers, member.(HashRingMember))
	}

	return hashRingMembers
}
