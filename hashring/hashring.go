package hashring

import (
	"errors"
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

type AddTask struct {
	member string
}

type RemoveTask struct {
	member string
}

type RingUpdateTask struct {
	Members    []string
	Partitions []int
	ResCh      chan interface{}
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

func MemberListtoStringList(listA []consistent.Member) []string {
	var strList []string

	for _, mem := range listA {
		strList = append(strList, mem.String())
	}

	return strList
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func (ring *Hashring) IsHealthy() error {
	if len(ring.GetMembers(false)) < ring.managerConfig.ReplicaCount && ring.getTaskListSize() > 0 {
		return errors.New("waiting on update nodes debounce")
	}
	return nil
}

func (ring *Hashring) getTaskListSize() int {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()
	return len(ring.taskList)
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

func (ring *Hashring) GetMembers(temp bool) []consistent.Member {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()

	return ring.getMembers(temp)
}

func (ring *Hashring) getMembers(temp bool) []consistent.Member {
	currMembers := ring.currConsistent.GetMembers()
	if temp {
		tempMembers := ring.tempConsistent.GetMembers()
		return mergeMemberList(currMembers, tempMembers)
	}
	return currMembers
}

func (ring *Hashring) GetMembersNames(temp bool) []string {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()
	return ring.getMembersNames(temp)
}

func (ring *Hashring) getMembersNames(temp bool) []string {
	members := ring.getMembers(temp)
	var memberNames []string
	for _, mem := range members {
		memberNames = append(memberNames, mem.String())
	}
	return memberNames
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
			ring.currConsistent.Add(CreateRingMember(task.member))
			ring.tempConsistent.Add(CreateRingMember(task.member))
		case RemoveTask:
			ring.currConsistent.Remove(task.member)
			ring.tempConsistent.Remove(task.member)
		default:
			logrus.Panicf("Hashring unknown task: %v", reflect.TypeOf(task))
		}
	}
	ring.taskList = []interface{}{}
	ring.notifyPartitionUpdate()
}

func (ring *Hashring) notifyPartitionUpdate() error {
	resCh := make(chan interface{})
	myPartitiosn, err := ring.getMyPartions()
	if err != nil {
		return err
	}
	ring.reqCh <- RingUpdateTask{ResCh: resCh, Partitions: myPartitiosn}
	<-resCh
	return nil
}

func (ring *Hashring) AddNode(member string) {
	ring.rwLock.Lock()
	defer ring.rwLock.Unlock()
	ring.taskList = append(ring.taskList, AddTask{member: member})
	ring.debounceUpdateRing()
}

func (ring *Hashring) RemoveNode(member string) {
	ring.rwLock.Lock()
	defer ring.rwLock.Unlock()
	ring.taskList = append(ring.taskList, RemoveTask{member: member})
	ring.debounceUpdateRing()
}

func (ring *Hashring) SetRingMembers(members, temp_members []string) {
	ring.rwLock.Lock()
	defer ring.rwLock.Unlock()
	var ringMembers []consistent.Member
	for _, mem := range members {
		ringMembers = append(ringMembers, CreateRingMember(mem))
	}

	var tempRingMembers []consistent.Member
	for _, mem := range temp_members {
		tempRingMembers = append(tempRingMembers, CreateRingMember(mem))
	}
	ring.tempConsistent = consistent.New(tempRingMembers, ring.consistentConfig)
	ring.currConsistent = consistent.New(ringMembers, ring.consistentConfig)
	ring.notifyPartitionUpdate()
}

func (ring *Hashring) CompareMembers(members, temp_members []string) bool {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()
	a := utils.CompareStringList(members, ring.GetMembersNames(false))
	b := utils.CompareStringList(temp_members, ring.GetMembersNames(true))
	logrus.Warnf("a %v b %v", a, b)
	return a && b
}

func (ring *Hashring) HasTempMembers() bool {
	ring.rwLock.RLock()
	defer ring.rwLock.RUnlock()
	return utils.CompareStringList(ring.GetMembersNames(true), ring.GetMembersNames(false)) == false
}

type RingMember struct {
	Name string
}

func CreateRingMember(name string) RingMember {
	return RingMember{Name: name}
}

func (m RingMember) String() string {
	return string(m.Name)
}
