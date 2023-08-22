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

var currEpoch int64 = 0

func main() {
	managerInit()
	// time.Sleep(20 * time.Second)

	defer func() {
		if r := recover(); r != nil {
			logrus.Warn("Recovered from panic:", r)
		}
	}()
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

	epochTick := time.NewTicker(epochTime)

	go startHttpServer()
	// var count uint32

	logrus.Warn("starting run.")
	tick := time.NewTicker(3 * time.Second)
	run := true
	for run {
		select {
		case <-tick.C:
			if raftNode.Leader() == "" {
				logrus.Warnf("NO LEADER! state = %s ME = %s err = %v", raftNode.State(), conf.Name, nil) // RaftTryLead()
				RaftTryLead()
			}
			if raftNode.State() != raft.Leader && raftNode.State() != raft.Follower {
				logrus.Warnf("BAD state = %s ME = %s", raftNode.State(), conf.Name)
			}

		case <-epochTick.C:
			if raftNode.State() != raft.Leader {
				continue
			}
			verifyErr := raftNode.VerifyLeader().Error()
			if verifyErr != nil {
				logrus.Warnf("verifyErr = %v", verifyErr)
				continue
			}
			UpdateEpoch()

		case data := <-delegate.msgCh:
			messageHolder, message, err := DecodeMessageHolder(data)
			if err != nil {
				logrus.Fatal(err)
			}
			message.Handle(messageHolder)

		case currEpoch = <-epochObserver:
			handleEpochUpdate(currEpoch)

		case isLeader := <-raftNode.LeaderCh():
			logrus.Warnf("leader change. %t %s %d", isLeader, raftNode.State(), currEpoch)
			if !isLeader {
				continue
			}
			for _, node := range clusterNodes.Members() {
				err := AddVoter(node.Name)
				if err != nil {
					logrus.Errorf("Testing AddVoter: %v", err)
					break
				}
			}
		}
	}

	// grpcStart()

	logrus.Warn("bye..............................")
}

var (
	raftDir      = "./raft-data"
	raftBindAddr = "127.0.0.1:7000" // 0.0.0.0:7000
)
