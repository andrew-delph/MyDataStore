package main

import (
	"log"

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

func (events *MyEventDelegate) Send(key, value string, replicas int) {

	setMsg := &SetMessage{
		Key:   key,
		Value: value,
	}

	nodeName, ok := events.consistent.GetNode(value)

	if !ok {
		log.Printf("no node available size=%d \n", events.consistent.Size())
		return
	}

	log.Printf("node1 search %s => %s", value, nodeName)

	node := events.nodes[nodeName]

	bytes, err := EncodeHolder(setMsg)

	if err != nil {
		log.Println("FAILED TO ENCODE", err)
		return
	}

	err = clusterNodes.SendReliable(node, bytes)

	if err != nil {
		log.Println("FAILED TO SEND", err)
		return
	}
}
