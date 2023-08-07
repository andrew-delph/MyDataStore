package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

var myMap sync.Map

func setValue(key string, value string) {
	myMap.Store(key, value)
}

func getValue(key string) (string, bool) {
	if value, ok := myMap.Load(key); ok {
		return value.(string), true
	}
	return "", false
}

var (
	clusterNodes *memberlist.Memberlist
	delegate     *MyDelegate
	events       *MyEventDelegate
	conf         *memberlist.Config
	ackMap       map[string]chan *MessageHolder
)

func main() {

	conf, delegate, events = GetConf()

	ackMap = make(map[string]chan *MessageHolder)

	var err error
	clusterNodes, err = memberlist.Create(conf)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	// Join an existing cluster by specifying at least one known member.
	n, err := clusterNodes.Join([]string{"store:8081"})
	if err != nil {
		panic("Failed to join cluster: " + err.Error())
	}

	log.Println("n", n)

	// Ask for members of the cluster
	for _, member := range clusterNodes.Members() {
		fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
	}

	tick := time.NewTicker(50000 * time.Millisecond)

	go startHttpServer()

	run := true
	for run {
		select {
		case <-tick.C:
			// value := randomString(5)

			// key := randomString(5)

			log.Println("TICK VALUE")

			// go events.SendSetMessage(key, value, 2)

		case data := <-delegate.msgCh:

			messageHolder, message, err := DecodeMessageHolder(data)
			if err != nil {
				log.Fatal(err)
			}

			message.Handle(messageHolder)

		}
	}

	log.Printf("bye..............................")
}
