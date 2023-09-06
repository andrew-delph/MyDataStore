package main

import (
	"time"

	"github.com/andrew-delph/my-key-store/config"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
)

var myPartions []int

var store Store

var globalEpoch int64 = 0

var theManager *Manager

func main() {
	var err error
	config := config.GetConfig()

	theManager, err = CreateManager(config)
	if err != nil {
		logrus.Fatal(err)
	}

	go StartProfileServer()
	go StartInterGrpcServer()
	logrus.SetLevel(logrus.WarnLevel)
	// logrus.SetFormatter(&logrus.JSONFormatter{})

	store, err = NewLevelDbStore(config.Manager)
	if err != nil {
		logrus.Fatalf("NewLevelDbStore err = %v", err)
	}
	defer store.Close()

	store.InitStore()

	SetupRaft(config.Raft)

	logrus.Infof("starting! %s", theManager.Config.Manager.Hostname)

	if err != nil {
		logrus.Panic("Failed to create memberlist: " + err.Error())
	}

	go startHttpServer()

	// Join an existing cluster by specifying at least one known member.
	for true {
		n, err := theManager.gossipCluster.cluster.Join(config.Gossip.InitMembers)
		logrus.Info("n", n)

		if err == nil {
			logrus.Info("JOINED MEMBERLIST CLUSTER")
			break
		}
		time.Sleep(5 * time.Second)
		logrus.Errorf("Failed to join cluster: my_addr = %v err = %v", theManager.gossipCluster.cluster.LocalNode().Addr, err)
	}

	// Ask for members of the cluster
	for _, member := range theManager.gossipCluster.cluster.Members() {
		logrus.Infof("Member: %s %s\n", member.Name, member.Addr)
	}

	epochTick := time.NewTicker(time.Duration(config.Raft.EpochTime))
	theManager.Run(config)

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

		case _ = <-theManager.gossipCluster.msgCh:
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
