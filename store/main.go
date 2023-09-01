package main

import (
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
)

var myPartions []int

var store Store

var globalEpoch int64 = 0

func main() {
	Init()
	// logrus.Warn("sleeping 1000 seconds.")
	// time.Sleep(1000 * time.Second)
	go StartProfileServer()
	go StartInterGrpcServer()
	logrus.SetLevel(logrus.WarnLevel)
	// logrus.SetFormatter(&logrus.JSONFormatter{})

	var err error

	store, err = NewLevelDbStore()
	if err != nil {
		logrus.Fatalf("NewLevelDbStore err = %v", err)
	}
	defer store.Close()

	store.InitStore()

	SetupRaft()

	logrus.Infof("starting! %s", hostname)

	conf, delegate, events = GetMemberlistConf()

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
				logrus.Errorf("UpdateEpoch err = %v", err)
				continue
			}

		case _ = <-delegate.msgCh:
			// handle memberlist message

		case isLeader := <-raftNode.LeaderCh():
			logrus.Debugf("leader change. %t %s %d", isLeader, raftNode.State(), globalEpoch)
			if !isLeader {
				continue
			}
			AddAllMembers()
			if globalEpoch == 0 {
				err = UpdateEpoch()
				if err != nil {
					logrus.Errorf("UpdateEpoch err = %v", err)
					continue
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
