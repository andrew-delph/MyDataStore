package main

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
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

var store Store

var globalEpoch int64 = 0

func main() {
	// logrus.Warn("sleeping 1000 seconds.")
	// time.Sleep(1000 * time.Second)
	go StartProfileServer()
	go StartInterGrpcServer()
	logrus.SetLevel(logrus.WarnLevel)
	// logrus.SetFormatter(&logrus.JSONFormatter{})

	data, err2 := os.ReadFile("/etc/hostname")
	if err2 != nil {
		logrus.Errorf("Error reading /etc/hostname: %v", err2)
		return
	}

	hostname = strings.TrimSpace(string(data))

	store, err2 = NewLevelDbStore()
	if err2 != nil {
		logrus.Fatalf("NewLevelDbStore: %v", err2)
	}
	defer store.Close()

	store.InitStore()

	SetupRaft()

	logrus.Infof("starting! %s", hostname)

	conf, delegate, events = GetMemberlistConf()

	var err error
	clusterNodes, err = memberlist.Create(conf)

	if err != nil {
		logrus.Panic("Failed to create memberlist: " + err.Error())
	}

	go startHttpServer()

	// Join an existing cluster by specifying at least one known member.
	for true {
		n, err := clusterNodes.Join(clusterJoinList)
		logrus.Info("n", n)

		if err == nil {
			logrus.Info("JOINED MEMBERLIST CLUSTER")
			break
		}
		time.Sleep(5 * time.Second)
		logrus.Errorf("Failed to join cluster: my_addr = %v err = %v", clusterNodes.LocalNode().Addr, err)
	}

	// Ask for members of the cluster
	for _, member := range clusterNodes.Members() {
		logrus.Infof("Member: %s %s\n", member.Name, member.Addr)
	}

	epochTick := time.NewTicker(epochTime)
	managerInit()

	tick := time.NewTicker(15 * time.Second)
	run := true
	for run {
		select {
		case <-tick.C:

			if raftNode.Leader() == "" || (raftNode.State() != raft.Leader && raftNode.State() != raft.Follower) {
				logrus.Warnf("BAD state = %s leader = %s num_peers = %v", raftNode.State(), raftNode.Leader(), raftNode.Stats()["num_peers"])
			} else {
				logrus.Debugf("state = %s num_peers = %v", raftNode.State(), raftNode.Stats()["num_peers"])
			}

		case <-epochTick.C:
			if raftNode.State() != raft.Leader {
				continue
			}
			err := raftNode.VerifyLeader().Error()
			if err != nil {
				logrus.Warnf("VerifyLeader err = %v", err)
				continue
			}
			err = UpdateEpoch()
			if err != nil {
				logrus.Warnf("UpdateEpoch err = %v", err)
				continue
			}

		case data := <-delegate.msgCh:
			messageHolder, message, err := DecodeMessageHolder(data)
			if err != nil {
				logrus.Fatal(err)
			}
			message.Handle(messageHolder)

		case isLeader := <-raftNode.LeaderCh():
			logrus.Debugf("leader change. %t %s %d", isLeader, raftNode.State(), globalEpoch)
			if !isLeader {
				continue
			}
			AddAllMembers()
		}
	}

	// grpcStart()

	logrus.Warn("bye..............................")
}

var (
	raftDir      = "./raft-data"
	raftBindAddr = "127.0.0.1:7000" // 0.0.0.0:7000
)
