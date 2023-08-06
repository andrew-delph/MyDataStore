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

func (d *MyEventDelegate) NotifyJoin(node *memberlist.Node) {
	log.Printf("join %s", node.Name)
	if d.consistent == nil {
		d.consistent = hashring.New([]string{node.Name})
	} else {
		d.consistent = d.consistent.AddNode(node.Name)
	}

	if d.nodes == nil {
		d.nodes = make(map[string]*memberlist.Node)
	}
	d.nodes[node.Name] = node

}
func (d *MyEventDelegate) NotifyLeave(node *memberlist.Node) {
	log.Printf("leave %s", node.Name)
	if d.consistent != nil {
		d.consistent = d.consistent.RemoveNode(node.Name)
	}

	if d.nodes != nil {
		delete(d.nodes, node.Name)
	}
}
func (d *MyEventDelegate) NotifyUpdate(node *memberlist.Node) {
	// skip
}

func (d *MyEventDelegate) Send(node string, m MyMessage) {
}
