package main

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
	"github.com/serialx/hashring"
)

type MyEventDelegate struct {
	consistent *hashring.HashRing
	nodes      map[string]*memberlist.Node
}

func GetMyEventDelegate() *MyEventDelegate {
	events := new(MyEventDelegate)

	events.consistent = hashring.New([]string{})

	events.nodes = make(map[string]*memberlist.Node)

	return events
}

func (events *MyEventDelegate) NotifyJoin(node *memberlist.Node) {

	log.Printf("join %s", node.Name)

	events.consistent = events.consistent.AddNode(node.Name)

	events.nodes[node.Name] = node
}

func (events *MyEventDelegate) NotifyLeave(node *memberlist.Node) {

	log.Printf("leave %s", node.Name)

	events.consistent = events.consistent.RemoveNode(node.Name)

	delete(events.nodes, node.Name)
}
func (events *MyEventDelegate) NotifyUpdate(node *memberlist.Node) {
	// skip
}

func (events *MyEventDelegate) SendSetMessage(key, value string, replicas int) error {

	ackId := uuid.New().String()

	setMsg := NewSetMessage(key, value, ackId)

	nodeName, ok := events.consistent.GetNode(value)

	if !ok {
		return fmt.Errorf("no node available size=%d", events.consistent.Size())
	}

	// log.Printf("node1 search %s => %s", value, nodeName)

	node := events.nodes[nodeName]

	bytes, err := EncodeHolder(setMsg)

	if err != nil {
		return fmt.Errorf("FAILED TO ENCODE: %v", err)
	}

	err = clusterNodes.SendReliable(node, bytes)

	if err != nil {
		return fmt.Errorf("FAILED TO SEND: %v", err)
	}

	return nil
}
