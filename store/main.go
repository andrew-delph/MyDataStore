package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

var myMap sync.Map

func set(key string, value string) {
	myMap.Store(key, value)
}

func get(key string) (string, bool) {
	if value, ok := myMap.Load(key); ok {
		return value.(string), true
	}
	return "", false
}

type MyMessage struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (m *MyMessage) Bytes() []byte {
	data, err := json.Marshal(m)
	if err != nil {
		return []byte("")
	}
	return data
}
func ParseMyMessage(data []byte) (*MyMessage, bool) {
	msg := new(MyMessage)
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, false
	}
	return msg, true
}

func main() {

	conf, delegate, events := GetConf()

	list, err := memberlist.Create(conf)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	// Join an existing cluster by specifying at least one known member.
	n, err := list.Join([]string{"store:8080"})
	if err != nil {
		panic("Failed to join cluster: " + err.Error())
	}

	log.Println("n", n)

	// Ask for members of the cluster
	for _, member := range list.Members() {
		fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
	}

	tick := time.NewTicker(500 * time.Millisecond)

	run := true
	for run {
		select {
		case <-tick.C:

			value := randomString(5)

			m := new(MyMessage)
			m.Key = "ping"
			m.Value = value

			nodeName, ok := events.consistent.GetNode(value)

			if ok {
				log.Printf("node1 search %s => %s", value, nodeName)
			} else {
				log.Printf("no node available")
			}

			node := events.nodes[nodeName]

			err := list.SendReliable(node, m.Bytes())

			if err != nil {
				log.Println("FAILED TO SEND", err)
			}

			// // ping to all
			// for _, node := range list.Members() {
			// 	if node.Name == conf.Name {
			// 		continue // skip self
			// 	}
			// 	log.Printf("send to %s msg: key=%s value=%d", node.Name, m.Key, m.Value)
			// 	list.SendReliable(node, m.Bytes())
			// }
		case data := <-delegate.msgCh:
			msg, ok := ParseMyMessage(data)
			if !ok {
				continue
			}

			log.Printf("received msg: key=%s value=%s", msg.Key, msg.Value)

			// if msg.Key == "ping" {
			// 	m := new(MyMessage)
			// 	m.Key = "pong"
			// 	m.Value = msg.Value + 1

			// 	devt := conf.Events.(*MyEventDelegate)
			// 	if devt == nil {
			// 		log.Printf("consistent isnt initialized")
			// 		continue
			// 	}
			// 	log.Printf("current node size: %d", devt.consistent.Size())

			// 	keys := []string{"hello", "world"}
			// 	for _, key := range keys {
			// 		node, ok := devt.consistent.GetNode(key)
			// 		if ok {
			// 			log.Printf("node1 search %s => %s", key, node)
			// 		} else {
			// 			log.Printf("no node available")
			// 		}
			// 	}

			// 	// pong to all
			// 	// list.SendToAddress()
			// 	for _, node := range list.Members() {
			// 		if node.Name == conf.Name {
			// 			continue // skip self
			// 		}
			// 		log.Printf("send to %s msg: key=%s value=%d", node.Name, m.Key, m.Value)
			// 		list.SendReliable(node, m.Bytes())
			// 	}
			// }
			// default:
			// 	log.Println("waiting...")
			// 	time.Sleep(5 * time.Second)
		}
	}

	log.Printf("bye..............................")
}
