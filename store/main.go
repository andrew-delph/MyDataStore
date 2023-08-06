package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/serialx/hashring"
)

type MyDelegate struct {
	msgCh chan []byte
}

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

func (d *MyDelegate) NotifyMsg(msg []byte) {
	d.msgCh <- msg
}
func (d *MyDelegate) NodeMeta(limit int) []byte {
	// not use, noop
	return []byte("")
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

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {
	msgCh := make(chan []byte)

	d := new(MyDelegate)
	d.msgCh = msgCh

	conf := memberlist.DefaultLocalConfig()

	conf.Logger = log.New(ioutil.Discard, "", 0)
	conf.BindPort = 8080
	conf.AdvertisePort = 8080
	conf.Delegate = d
	conf.Events = new(MyEventDelegate)

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

			devt := conf.Events.(*MyEventDelegate)

			nodeName, ok := devt.consistent.GetNode(value)

			if ok {
				log.Printf("node1 search %s => %s", value, nodeName)
			} else {
				log.Printf("no node available")
			}

			node := devt.nodes[nodeName]

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
		case data := <-d.msgCh:
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
