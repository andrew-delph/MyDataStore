package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"
)

type MyEventDelegate struct {
	consistent *consistent.Consistent
	nodes      map[string]*memberlist.Node
}

var partitionVerified = make(map[int]bool)

func GetMyEventDelegate() *MyEventDelegate {
	events := new(MyEventDelegate)

	events.consistent = GetHashRing()

	events.nodes = make(map[string]*memberlist.Node)

	return events
}

func (events *MyEventDelegate) NotifyJoin(node *memberlist.Node) {

	logrus.Infof("join %s", node.Name)

	AddNode(events.consistent, node.Name)

	events.nodes[node.Name] = node
	var err error
	myPartions, err = GetMemberPartions(events.consistent, hostname)
	if err != nil {
		logrus.Error(err)
		return
	}
	logrus.Debugf("myPartions %v", myPartions)
	LoadPartitions(myPartions)
}

func (events *MyEventDelegate) NotifyLeave(node *memberlist.Node) {

	logrus.Infof("leave %s", node.Name)

	RemoveNode(events.consistent, node.Name)

	delete(events.nodes, node.Name)
	var err error
	myPartions, err = GetMemberPartions(events.consistent, hostname)
	if err != nil {
		logrus.Error(err)
		return
	}
	logrus.Debugf("myPartions %v", myPartions)
	LoadPartitions(myPartions)
}
func (events *MyEventDelegate) NotifyUpdate(node *memberlist.Node) {
	// skip
}

func (events *MyEventDelegate) SendSetMessage(key, value string) error {

	ackId := uuid.New().String()

	setMsg := NewSetMessage(key, value, ackId)

	nodes, err := GetClosestN(events.consistent, key, totalReplicas)
	if err != nil {
		return err
	}

	ackChannel := make(chan *MessageHolder, totalReplicas)
	defer close(ackChannel)

	setAckChannel(ackId, ackChannel)
	defer deleteAckChannel(ackId)

	for _, node := range nodes {

		logrus.Debugf("node1 search %s => %s", key, node.String())

		node := events.nodes[node.String()]

		bytes, err := EncodeHolder(setMsg)

		if err != nil {
			return fmt.Errorf("FAILED TO ENCODE: %v", err)
		}

		err = clusterNodes.SendReliable(node, bytes)

		if err != nil {
			return fmt.Errorf("FAILED TO SEND: %v", err)
		}
	}

	ackSet := make(map[string]bool)

	timeout := time.After(defaultTimeout)

	for {
		select {
		case ackMessageHolder := <-ackChannel:
			ackSet[ackMessageHolder.SenderName] = true
			if len(ackSet) >= writeResponse {
				return nil
			}
		case <-timeout:
			logrus.Debug("SET TIME OUT REACHED!!! len(ackSet) = ", len(ackSet))
			return fmt.Errorf("timeout waiting for acknowledgements")
		}
	}

}

func (events *MyEventDelegate) SendGetMessage(key string) (string, error) {

	ackId := uuid.New().String()

	getMsg := NewGetMessage(key, ackId)

	nodes, err := GetClosestN(events.consistent, key, totalReplicas)
	if err != nil {
		return "", err
	}

	ackChannel := make(chan *MessageHolder, totalReplicas)
	defer close(ackChannel)

	setAckChannel(ackId, ackChannel)
	defer deleteAckChannel(ackId)

	for _, node := range nodes {

		node := events.nodes[node.String()]

		bytes, err := EncodeHolder(getMsg)

		if err != nil {
			return "", fmt.Errorf("FAILED TO ENCODE: %v", err)
		}

		err = clusterNodes.SendReliable(node, bytes)

		if err != nil {
			return "", fmt.Errorf("FAILED TO SEND: %v", err)
		}
	}

	ackSet := make(map[string]int)

	timeout := time.After(defaultTimeout)

	for {
		select {
		case ackMessageHolder := <-ackChannel:

			ackMessage := &AckMessage{}
			err := ackMessage.Decode(ackMessageHolder.MessageBytes)
			if err != nil {
				return "", fmt.Errorf("failed to Decode AckMessage: %v", err)
			}

			ackValue := ackMessage.Value
			ackSet[ackValue]++

			if ackSet[ackValue] == readResponse {
				return ackValue, nil
			}
		case <-timeout:
			logrus.Warn("TIME OUT REACHED!!!")
			return "", fmt.Errorf("timeout waiting for acknowledgements")
		}
	}

}

func (events *MyEventDelegate) SendRequestPartitionInfoMessage(hash []byte, partitionId int) error {

	partitionTree, err := PartitionMerkleTree(partitionId)
	if err != nil {
		logrus.Debug(err)
		return err
	}

	ackId := uuid.New().String()

	partitionMsg := NewRequestPartitionInfo(ackId, partitionId)

	nodes, err := GetClosestNForPartition(events.consistent, partitionId, totalReplicas)
	if err != nil {
		logrus.Debug(err)
		return err
	}

	ackChannel := make(chan *MessageHolder, totalReplicas)
	defer close(ackChannel)

	setAckChannel(ackId, ackChannel)
	defer deleteAckChannel(ackId)

	for _, node := range nodes {

		node := events.nodes[node.String()]

		bytes, err := EncodeHolder(partitionMsg)

		if err != nil {
			return fmt.Errorf("FAILED TO ENCODE: %v", err)
		}

		err = clusterNodes.SendReliable(node, bytes)

		if err != nil {
			return fmt.Errorf("FAILED TO SEND: %v", err)
		}
	}

	ackSet := make(map[string]bool)

	timeout := time.After(time.Second * 10)

	for {
		select {
		case ackMessageHolder := <-ackChannel:

			responsePartitionInfo := &ResponsePartitionInfo{}
			err := responsePartitionInfo.Decode(ackMessageHolder.MessageBytes)
			if err != nil {
				logrus.Errorf("failed to Decode ResponsePartitionInfo: %v", err)
				return fmt.Errorf("failed to Decode ResponsePartitionInfo: %v", err)
			}

			areEqual := bytes.Equal(partitionTree.Root.Hash, responsePartitionInfo.Hash)

			if areEqual {
				ackSet[ackMessageHolder.SenderName] = true
			}

			if len(ackSet) >= readResponse {
				logrus.Debugf("The partition %d is healthy. health neighbors are %d", partitionId, len(ackSet))

				partitionVerified[partitionId] = true
				return nil
			}
		case <-timeout:

			keys := make([]string, 0, len(ackSet))
			for key := range ackSet {
				keys = append(keys, key)
			}

			logrus.Warnf("The partition %d is not healthy. health neighbors are %v", partitionId, keys)

			partitionVerified[partitionId] = false
			return fmt.Errorf("timeout waiting for acknowledgements")
		}
	}

}

func (events *MyEventDelegate) SendResponsePartitionInfo(ackId, senderName string, partitionId int) error {

	partitionTree, err := PartitionMerkleTree(partitionId)
	if err != nil {
		logrus.Debug(err)
		return err
	}

	value, exists := partitionVerified[partitionId]

	if !exists {
		value = false
	}

	ackMsg := NewResponsePartitionInfo(ackId, partitionTree.Root.Hash, value, partitionId)

	node := events.nodes[senderName]

	bytes, err := EncodeHolder(ackMsg)

	if err != nil {
		return fmt.Errorf("FAILED TO ENCODE: %v", err)
	}

	err = clusterNodes.SendReliable(node, bytes)

	if err != nil {
		return fmt.Errorf("FAILED TO SEND: %v", err)
	}

	return nil
}

func (events *MyEventDelegate) SendAckMessage(value, ackId, senderName string, success bool) error {

	ackMsg := NewAckMessage(ackId, success, value)

	node := events.nodes[senderName]

	bytes, err := EncodeHolder(ackMsg)

	if err != nil {
		return fmt.Errorf("FAILED TO ENCODE: %v", err)
	}

	err = clusterNodes.SendReliable(node, bytes)

	if err != nil {
		return fmt.Errorf("FAILED TO SEND: %v", err)
	}

	return nil
}
