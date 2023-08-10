package main

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"
)

var (
	clusterNodes *memberlist.Memberlist
	delegate     *MyDelegate
	events       *MyEventDelegate
	conf         *memberlist.Config
	ackMap       sync.Map
)

func setAckChannel(key string, ch chan *MessageHolder) {
	ackMap.Store(key, ch)
}

func getAckChannel(key string) (chan *MessageHolder, bool) {
	if value, ok := ackMap.Load(key); ok {
		return value.(chan *MessageHolder), true
	}
	return nil, false
}

func deleteAckChannel(key string) {
	ackMap.Delete(key)
}

var (
	hostname   string
	myPartions []int
)

func main() {
	go StartInterGrpcServer()
	logrus.SetLevel(logrus.WarnLevel)
	// logrus.SetFormatter(&logrus.JSONFormatter{})

	InitStore()

	data, err2 := os.ReadFile("/etc/hostname")
	if err2 != nil {
		logrus.Errorf("Error reading /etc/hostname: %v", err2)
		return
	}

	hostname = strings.TrimSpace(string(data))

	logrus.Infof("starting! %s", hostname)

	conf, delegate, events = GetConf()

	var err error
	clusterNodes, err = memberlist.Create(conf)
	if err != nil {
		logrus.Panic("Failed to create memberlist: " + err.Error())
	}

	// Join an existing cluster by specifying at least one known member.
	n, err := clusterNodes.Join([]string{"store:8081"})
	if err != nil {
		logrus.Panic("Failed to join cluster: " + err.Error())
	}

	logrus.Info("n", n)

	// Ask for members of the cluster
	for _, member := range clusterNodes.Members() {
		logrus.Infof("Member: %s %s\n", member.Name, member.Addr)
	}

	tick := time.NewTicker(50000 * time.Millisecond)

	go startHttpServer()

	// verify partitions every x seconds

	partitionTimer := time.NewTicker(1000 * time.Millisecond)
	go func() {
		for range partitionTimer.C {
			Client()
			myPartions, err := GetMemberPartions(events.consistent, hostname)
			if err != nil {
				logrus.Warn(err)
				continue
			}

			for _, partitionId := range myPartions {
				partitionTree, err := PartitionMerkleTree(partitionId)
				if err != nil {
					logrus.Debug(err)
					continue
				}
				events.SendRequestPartitionInfoMessage(partitionTree.Root.Hash, partitionId)
			}
		}
	}()

	run := true
	for run {
		select {
		case <-tick.C:
			// value := randomString(5)

			// key := randomString(5)

			logrus.Debug("TICK VALUE")

			// go events.SendSetMessage(key, value, 2)

		case data := <-delegate.msgCh:

			messageHolder, message, err := DecodeMessageHolder(data)
			if err != nil {
				logrus.Fatal(err)
			}

			message.Handle(messageHolder)

		}
	}

	// grpcStart()

	logrus.Info("bye..............................")
}
