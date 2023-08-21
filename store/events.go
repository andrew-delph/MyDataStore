package main

import (
	"fmt"

	"github.com/buraksezer/consistent"
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
	err = AddVoter(node.Name)
	if err != nil {
		logrus.Errorf("add voter err = %v", err)
	}

	// myPartions, err = GetMemberPartions(events.consistent, conf.Name)
	// if err != nil {
	// 	logrus.Error(err)
	// 	return
	// }

	// logrus.Debugf("myPartions %v", myPartions)
	// store.LoadPartitions(myPartions)
}

func (events *MyEventDelegate) NotifyLeave(node *memberlist.Node) {
	logrus.Infof("leave %s", node.Name)

	RemoveNode(events.consistent, node.Name)

	delete(events.nodes, node.Name)
	var err error

	err = RemoveServer(node.Name)
	if err != nil {
		logrus.Errorf("remove server err = %v", err)
		return
	}
	// Snapshot()
	logrus.Warn("SUCCESS")

	// myPartions, err = GetMemberPartions(events.consistent, conf.Name)
	// if err != nil {
	// 	logrus.Error(err)
	// 	return
	// }
	// logrus.Debugf("myPartions %v", myPartions)
	// store.LoadPartitions(myPartions)
}

func (events *MyEventDelegate) NotifyUpdate(node *memberlist.Node) {
	logrus.Warnf("NotifyUpdate %s", node.Name)
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
