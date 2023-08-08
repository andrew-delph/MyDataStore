package main

import (
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/sirupsen/logrus"
)

var hashRingConf = consistent.Config{
	PartitionCount:    10,
	ReplicationFactor: 40,
	Load:              1.2,
	Hasher:            hasher{},
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type HashRingMember string

func (m HashRingMember) String() string {
	return string(m)
}

func GetHashRing() *consistent.Consistent {

	hashring := consistent.New(nil, hashRingConf)
	return hashring
}

func AddNode(hashring *consistent.Consistent, nodeName string) {
	member := HashRingMember(nodeName)
	hashring.Add(member)
}

func RemoveNode(hashring *consistent.Consistent, member string) {
	hashring.Remove(member)

}

func FindPartitionID(hashring *consistent.Consistent, key string) int {
	keyBytes := []byte(key)
	return hashring.FindPartitionID(keyBytes)
}

func GetMemberPartions(hashring *consistent.Consistent, member string) []int {
	var belongsTo []int
	for partID := 0; partID < hashRingConf.PartitionCount; partID++ {
		members, err := hashring.GetClosestNForPartition(partID, totalReplicas)
		if err != nil {
			logrus.Panic(err)
		}
		partitionMembers := ConvertHashRingMemberArray(members)
		for _, partitionMember := range partitionMembers {
			if partitionMember.String() == member {
				belongsTo = append(belongsTo, partID)
			}
		}
	}
	return belongsTo
}

func GetMembers(hashring *consistent.Consistent) []HashRingMember {
	return ConvertHashRingMemberArray(hashring.GetMembers())
}

func GetClosestN(hashring *consistent.Consistent, key string, count int) ([]HashRingMember, error) {
	keyBytes := []byte(key)

	members, err := hashring.GetClosestN(keyBytes, count)
	if err != nil {
		return nil, err
	}

	return ConvertHashRingMemberArray(members), nil
}

func GetClosestNForPartition(hashring *consistent.Consistent, partitionId, count int) ([]HashRingMember, error) {

	members, err := hashring.GetClosestNForPartition(partitionId, count)

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
