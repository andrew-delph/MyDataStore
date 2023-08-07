package main

import (
	"fmt"
	"log"
	"time"

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

	nodes, ok := events.consistent.GetNodes(key, replicas)

	ackChannel := make(chan *MessageHolder, replicas)
	defer close(ackChannel)

	setAckChannel(ackId, ackChannel)
	defer deleteAckChannel(ackId)

	if !ok {
		return fmt.Errorf("no node available size=%d", events.consistent.Size())
	}

	for _, nodeName := range nodes {

		log.Printf("node1 search %s => %s", key, nodeName)

		node := events.nodes[nodeName]

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

	timeout := time.After(2 * time.Second)

	for len(ackSet) < replicas {
		select {
		case ackMessageHolder := <-ackChannel:
			ackSet[ackMessageHolder.SenderName] = true
		case <-timeout:
			log.Println("TIME OUT REACHED!!!")
			return fmt.Errorf("timeout waiting for acknowledgements")
		}
	}

	log.Println("SET ACK COMPLETE", replicas)

	return nil
}

func (events *MyEventDelegate) SendGetMessage(key string, replicas int) (string, error) {

	ackId := uuid.New().String()

	getMsg := NewGetMessage(key, ackId)

	nodes, ok := events.consistent.GetNodes(key, replicas)

	ackChannel := make(chan *MessageHolder, replicas)
	defer close(ackChannel)

	setAckChannel(ackId, ackChannel)
	defer deleteAckChannel(ackId)

	if !ok {
		return "", fmt.Errorf("no node available size=%d", events.consistent.Size())
	}

	for _, nodeName := range nodes {

		node := events.nodes[nodeName]

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

	timeout := time.After(2 * time.Second)

	for len(ackSet) < replicas {
		select {
		case ackMessageHolder := <-ackChannel:

			ackMessage := &AckMessage{}
			err := ackMessage.Decode(ackMessageHolder.MessageBytes)
			if err != nil {
				return "", fmt.Errorf("failed to Decode AckMessage: %v", err)
			}

			ackValue := ackMessage.Value
			ackSet[ackValue]++

			if ackSet[ackValue] == replicas {
				return ackValue, nil
			}
		case <-timeout:
			log.Println("TIME OUT REACHED!!!")
			return "", fmt.Errorf("timeout waiting for acknowledgements")
		}
	}

	log.Println("GET ACK COMPLETE", replicas)

	return "", fmt.Errorf("value not found for key= %s", key)
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
