package hashring

import (
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
)

func testHashring() {
	logrus.Warn("HASRING")
}

type Hashring struct {
	hashringConfig   config.HashringConfig
	consistentConfig consistent.Config
	consistent       *consistent.Consistent
}

func CreateHashring(hashringConfig config.HashringConfig) Hashring {
	consistentConfig := consistent.Config{
		PartitionCount:    hashringConfig.PartitionCount,
		ReplicationFactor: 13,
		Load:              1.2,
		Hasher:            hasher{},
	}
	Consistent := consistent.New(nil, consistentConfig)
	return Hashring{hashringConfig: hashringConfig, consistent: Consistent, consistentConfig: consistentConfig}
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func (ring *Hashring) AddNode(member consistent.Member) {
	ring.consistent.Add(member)
}

func (ring *Hashring) RemoveNode(name string) {
	ring.consistent.Remove(name)
}

func (ring *Hashring) FindPartitionID(key []byte) int {
	return ring.consistent.FindPartitionID(key)
}

func (ring *Hashring) GetMemberPartions(member string) ([]int, error) {
	var belongsTo []int
	for partID := 0; partID < ring.consistentConfig.PartitionCount; partID++ {
		members, err := ring.consistent.GetClosestNForPartition(partID, ring.consistentConfig.PartitionCount)
		if err != nil {
			return nil, err
		}
		for _, partitionMember := range members {
			if partitionMember.String() == member {
				belongsTo = append(belongsTo, partID)
			}
		}
	}
	return belongsTo, nil
}

func (ring Hashring) GetMembers() []consistent.Member {
	return ring.consistent.GetMembers()
}

func (ring Hashring) GetClosestN(key string, count int) ([]consistent.Member, error) {
	keyBytes := []byte(key)

	return ring.consistent.GetClosestN(keyBytes, count)
}

func (ring Hashring) GetClosestNForPartition(partitionId, count int) ([]consistent.Member, error) {
	return ring.consistent.GetClosestNForPartition(partitionId, count)
}
