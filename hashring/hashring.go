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
	"github.com/andrew-delph/my-key-store/utils"
)

type Hashring struct {
	managerConfig    config.ManagerConfig
	consistentConfig consistent.Config
	currConsistent   *consistent.Consistent
	tempConsistent   *consistent.Consistent

	rwLock    *sync.RWMutex
	debouncer func(f func())
	taskList  []interface{}
	reqCh     chan interface{}
}

func CreateHashring(managerConfig config.ManagerConfig, reqCh chan interface{}) *Hashring {
	consistentConfig := consistent.Config{
		PartitionCount:    managerConfig.PartitionCount,
		ReplicationFactor: managerConfig.PartitionReplicas,
		Load:              managerConfig.Load,
		Hasher:            hasher{},
	}
	currConsistent := consistent.New(nil, consistentConfig)
	tempConsistent := consistent.New(nil, consistentConfig)

	debouncer := debounce.New(time.Duration(managerConfig.RingDebounce * float64(time.Second)))
	var rwLock sync.RWMutex
	return &Hashring{managerConfig: managerConfig, currConsistent: currConsistent, tempConsistent: tempConsistent, consistentConfig: consistentConfig, rwLock: &rwLock, debouncer: debouncer, reqCh: reqCh}
}

func mergeMemberList(listA, listB []consistent.Member) []consistent.Member {
	mSet := make(map[string]consistent.Member)

	for _, mem := range listA {
		mSet[mem.String()] = mem
	}

	for _, mem := range listB {
		mSet[mem.String()] = mem
	}

	var res []consistent.Member

	for _, mem := range mSet {
		res = append(res, mem)
	}

	return res
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func (ring *Hashring) FindPartitionID(key []byte) int {
	return ring.currConsistent.FindPartitionID(key)
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
	belongsTo := utils.NewIntSet()
	// add from currConsistent
	for partID := 0; partID < ring.managerConfig.PartitionCount; partID++ {
		members, err := ring.currConsistent.GetClosestNForPartition(partID, ring.managerConfig.ReplicaCount)
		if err != nil {
			return nil, err
		}
		for _, partitionMember := range members {
			if partitionMember.String() == member {
				belongsTo.Add(partID)
			}
		}
	}

	// add from tempConsistent
	for partID := 0; partID < ring.managerConfig.PartitionCount; partID++ {
		members, err := ring.tempConsistent.GetClosestNForPartition(partID, ring.managerConfig.ReplicaCount)
		if err != nil {
			return nil, err
		}
		for _, partitionMember := range members {
			if partitionMember.String() == member {
				belongsTo.Add(partID)
			}
		}
	}
	return belongsTo.List(), nil
}

func (ring *Hashring) GetCurrMembers() []consistent.Member {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()
	return ring.currConsistent.GetMembers()
}

func (ring *Hashring) GetMembers() []consistent.Member {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()
	currMembers := ring.currConsistent.GetMembers()
	tempMembers := ring.tempConsistent.GetMembers()
	return mergeMemberList(currMembers, tempMembers)
}

func (ring *Hashring) GetClosestN(key string, count int, includeSelf bool) ([]consistent.Member, error) {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()
	keyBytes := []byte(key)

	currMembers, err := ring.currConsistent.GetClosestN(keyBytes, count)
	if err != nil {
		return nil, err
	}
	tempMembers, err := ring.tempConsistent.GetClosestN(keyBytes, count)
	if err != nil {
		return nil, err
	}
	members := mergeMemberList(currMembers, tempMembers)
	if includeSelf {
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
	currMembers, err := ring.currConsistent.GetClosestNForPartition(partitionId, count)
	if err != nil {
		return nil, err
	}
	tempMembers, err := ring.tempConsistent.GetClosestNForPartition(partitionId, count)
	if err != nil {
		return nil, err
	}

	members := mergeMemberList(currMembers, tempMembers)

	if includeSelf {
		return members, nil
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

func (ring *Hashring) debounceUpdateRing() {
	ring.debouncer(ring.UpdateRing)
}

func (ring *Hashring) UpdateRing() {
	ring.rwLock.Lock()
	defer ring.rwLock.Unlock()
	for _, rawTask := range ring.taskList {
		switch task := rawTask.(type) {
		case AddTask:
			ring.currConsistent.Add(task.member)
			ring.tempConsistent.Add(task.member)
		case RemoveTask:
			ring.currConsistent.Remove(task.name)
			ring.tempConsistent.Remove(task.name)
		default:
			logrus.Panicf("Hashring unknown task: %v", reflect.TypeOf(task))
		}
	}
	ring.notifyPartitionUpdate()
}

func (ring *Hashring) notifyPartitionUpdate() error {
	myPartitions, err := ring.getMyPartions()
	resCh := make(chan interface{})
	if err != nil {
		return err
	} else {
		ring.reqCh <- PartitionsUpdateTask{MyPartitions: myPartitions, ResCh: resCh}
	}
	<-resCh
	return nil
}

type AddTask struct {
	member consistent.Member
}

type RemoveTask struct {
	name string
}

type PartitionsUpdateTask struct {
	MyPartitions []int
	ResCh        chan interface{}
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

func (ring *Hashring) AddTempNode(member consistent.Member) error {
	ring.resetTempRing()
	ring.rwLock.Lock()
	defer ring.rwLock.Unlock()
	ring.tempConsistent.Add(member)
	return ring.notifyPartitionUpdate()
}

func (ring *Hashring) RemoveTempNode(name string) error {
	ring.resetTempRing()
	ring.rwLock.Lock()
	defer ring.rwLock.Unlock()
	ring.tempConsistent.Remove(name)
	return ring.notifyPartitionUpdate()
}

func (ring *Hashring) resetTempRing() {
	ring.rwLock.Lock()
	defer ring.rwLock.Unlock()
	currMembers := ring.currConsistent.GetMembers()
	ring.tempConsistent = consistent.New(currMembers, ring.consistentConfig)
}
