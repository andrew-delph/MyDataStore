package main

import (
	"encoding/json"
	"fmt"

	"github.com/buraksezer/consistent"
	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"
)

type NodeState struct {
	Health bool
}

type MyDelegate struct {
	msgCh chan []byte
	state *NodeState
}

func GetMyDelegate() *MyDelegate {
	delegate := &MyDelegate{state: &NodeState{Health: validFSM}}
	delegate.msgCh = make(chan []byte)
	return delegate
}

func UpdateNodeHealth(health bool) error {
	logrus.Debug("UpdateNodeHealth >> ", health)
	delegate.state.Health = health
	err := clusterNodes.UpdateNode(0)
	if err != nil {
		logrus.Errorf("UpdateNode err = %v", err)
	}
	return err
}

func (d *MyDelegate) NotifyMsg(msg []byte) {
	d.msgCh <- msg
}

func (d *MyDelegate) NodeMeta(limit int) []byte {
	data, err := json.Marshal(d.state)
	if err != nil {
		logrus.Errorf("NodeMeta err = %v", err)
		return nil
	}
	logrus.Debug("NodeMeta >> ", d.state.Health)
	return data
}

func (d *MyDelegate) LocalState(join bool) []byte {
	// not use, noop
	return []byte("")
}

func (d *MyDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	// not use, noop
	return nil
}

func (d *MyDelegate) MergeRemoteState(buf []byte, join bool) {
	// not use
}

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
	var err error
	logrus.Infof("join %s", node.Name)

	err = HandleNotifyUpdate(node)
	if err != nil {
		logrus.Errorf("NotifyLeave err = %v", err)
		return
	}

	events.nodes[node.Name] = node
	err = AddVoter(node)
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
	var err error

	logrus.Infof("leave %s", node.Name)

	err = HandleNotifyUpdate(node)
	if err != nil {
		logrus.Errorf("NotifyLeave err = %v", err)
		return
	}

	RemoveNode(events.consistent, node)

	delete(events.nodes, node.Name)

	err = RemoveServer(node)
	if err != nil {
		logrus.Errorf("remove server err = %v", err)
		return
	}

	// myPartions, err = GetMemberPartions(events.consistent, conf.Name)
	// if err != nil {
	// 	logrus.Error(err)
	// 	return
	// }
	// logrus.Debugf("myPartions %v", myPartions)
	// store.LoadPartitions(myPartions)
}

func (events *MyEventDelegate) NotifyUpdate(node *memberlist.Node) {
	err := HandleNotifyUpdate(node)
	if err != nil {
		logrus.Errorf("NotifyUpdate err = %v", err)
		return
	}
}

func HandleNotifyUpdate(node *memberlist.Node) error {
	var otherNode NodeState
	err := json.Unmarshal(node.Meta, &otherNode)
	if err != nil {
		logrus.Errorf("HandleNotifyUpdate error deserializing. err = %v", err)
		return err
	}

	if otherNode.Health {
		AddNode(events.consistent, node)
	} else {
		RemoveNode(events.consistent, node)
	}

	logrus.Warnf("HandleNotifyUpdate name = %s Health = %v", node.Name, otherNode.Health)
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
