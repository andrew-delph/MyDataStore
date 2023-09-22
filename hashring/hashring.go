package hashring

import (
	"reflect"
	"sync"
	"time"

	"github.com/bep/debounce"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
)

type Hashring struct {
	managerConfig    config.ManagerConfig
	consistentConfig consistent.Config
	consistent       *consistent.Consistent
	rwLock           *sync.RWMutex
	debouncer        func(f func())
	taskList         []interface{}
	reqCh            chan interface{}
}

func CreateHashring(managerConfig config.ManagerConfig, reqCh chan interface{}) *Hashring {
	consistentConfig := consistent.Config{
		PartitionCount:    managerConfig.PartitionCount,
		ReplicationFactor: managerConfig.PartitionReplicas,
		Load:              managerConfig.Load,
		Hasher:            hasher{},
	}
	Consistent := consistent.New(nil, consistentConfig)

	debouncer := debounce.New(time.Duration(managerConfig.RingDebounce * float64(time.Second)))
	var rwLock sync.RWMutex
	return &Hashring{managerConfig: managerConfig, consistent: Consistent, consistentConfig: consistentConfig, rwLock: &rwLock, debouncer: debouncer, reqCh: reqCh}
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func (ring *Hashring) FindPartitionID(key []byte) int {
	return ring.consistent.FindPartitionID(key)
}

func (ring *Hashring) GetMyPartions() ([]int, error) {
	return ring.GetMemberPartions(ring.managerConfig.Hostname)
}

func (ring *Hashring) getMyPartions() ([]int, error) {
	return ring.getMemberPartions(ring.managerConfig.Hostname)
}

func (ring *Hashring) GetMemberPartions(member string) ([]int, error) {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()
	return ring.getMemberPartions(member)
}

func (ring *Hashring) getMemberPartions(member string) ([]int, error) {
	var belongsTo []int
	for partID := 0; partID < ring.consistentConfig.PartitionCount; partID++ {
		members, err := ring.consistent.GetClosestNForPartition(partID, ring.consistentConfig.ReplicationFactor)
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

func (ring *Hashring) GetMembers() []consistent.Member {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()
	return ring.consistent.GetMembers()
}

func (ring *Hashring) GetClosestN(key string, count int, includeSelf bool) ([]consistent.Member, error) {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()
	keyBytes := []byte(key)

	members, err := ring.consistent.GetClosestN(keyBytes, count)
	if includeSelf || err != nil {
		return members, err
	}

	var membersFilter []consistent.Member
	for _, member := range members {
		// add everyone but self
		if ring.managerConfig.Hostname != member.String() {
			membersFilter = append(membersFilter, member)
		}
	}
	return membersFilter, nil
}

func (ring *Hashring) GetClosestNForPartition(partitionId, count int, includeSelf bool) ([]consistent.Member, error) {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()
	members, err := ring.consistent.GetClosestNForPartition(partitionId, count)
	if includeSelf || err != nil {
		return members, err
	}

	var membersFilter []consistent.Member
	for _, member := range members {
		// add everyone but self
		if ring.managerConfig.Hostname != member.String() {
			membersFilter = append(membersFilter, member)
		}
	}
	return ring.consistent.GetClosestNForPartition(partitionId, count)
}

func (ring *Hashring) debounceUpdateRing() {
	ring.debouncer(ring.updateRing)
}

func (ring *Hashring) updateRing() {
	ring.rwLock.Lock()
	defer ring.rwLock.Unlock()
	for _, rawTask := range ring.taskList {
		switch task := rawTask.(type) {
		case AddTask:
			ring.consistent.Add(task.member)
		case RemoveTask:
			ring.consistent.Remove(task.name)
		default:
			logrus.Panicf("Hashring unknown task: %v", reflect.TypeOf(task))
		}
	}

	myPartitions, err := ring.getMyPartions()

	if err != nil {
		logrus.Errorf("Hashring updateRing err = %v", err)
	} else {
		ring.reqCh <- PartitionsUpdate{MyPartitions: myPartitions}
	}
}

type AddTask struct {
	member consistent.Member
}

type RemoveTask struct {
	name string
}

type PartitionsUpdate struct {
	MyPartitions []int
}

func (ring *Hashring) AddNode(member consistent.Member) {
	ring.rwLock.Lock()
	defer ring.rwLock.Unlock()
	ring.taskList = append(ring.taskList, AddTask{member: member})
	ring.debounceUpdateRing()
}

func (ring *Hashring) RemoveNode(name string) {
	ring.rwLock.Lock()
	defer ring.rwLock.Unlock()
	ring.taskList = append(ring.taskList, RemoveTask{name: name})
	ring.debounceUpdateRing()
}
