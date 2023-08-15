package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/sirupsen/logrus"
)

var raftNode *raft.Raft

type StateMachine struct {
	Epoch uint64
}

var epochObserver = make(chan uint64, 1)

func (sm *StateMachine) Apply(logEntry *raft.Log) interface{} {
	epoch := binary.BigEndian.Uint64(logEntry.Data)
	sm.Epoch = epoch
	epochObserver <- epoch
	return nil
}

func (sm *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	// Implement snapshot functionality if needed
	return nil, nil
}

func (sm *StateMachine) Restore(serialized io.ReadCloser) error {
	// Implement restoring state from a snapshot if needed
	return nil
}

var fsm *StateMachine

func SetupRaft() {
	hostname, exists := os.LookupEnv("HOSTNAME")
	if !exists {
		logrus.Fatal("hostname env not set.")
	} else {
		logrus.Infof("hostname='%s'", hostname)
	}

	localIp, err := getLocalIP()
	if err != nil {
		logrus.Fatal("getLocalIP failed", err)
	} else {
		logrus.Warnf("localIp='%s'", localIp)
	}

	raftBindAddr = fmt.Sprintf("%s:7000", localIp)

	raftDir = fmt.Sprintf("./raft_%s", hostname)

	// Clean up the previous state
	os.RemoveAll(raftDir)

	// Create the 'raft-data' directory if it doesn't exist
	if err := os.MkdirAll(raftDir, 0o700); err != nil {
		logrus.Fatal(err)
	}

	// Create a network transport for raftNode1
	transport, err := getTransport(raftBindAddr)
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a configuration for raftNode1
	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(fmt.Sprintf("%s:7000", localIp))

	raftLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "discard",
		Output: ioutil.Discard,
		Level:  hclog.NoLevel,
	})

	raftConf.Logger = raftLogger

	raftConf.LogLevel = "ERROR"

	// Create a store to persist Raft logrus entries
	store, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stdout)
	if err != nil {
		logrus.Fatal(err)
	}

	fsm = &StateMachine{} // Your state machine instance

	// Create raftNode
	raftNode, err = raft.NewRaft(raftConf, fsm, store, store, snapshotStore, transport)
	if err != nil {
		logrus.Fatal(err)
	}

	// Bootstrap the leader (raftNode1)
	bootstrapFuture := raftNode.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raftConf.LocalID,
				Address:  raft.ServerAddress(transport.LocalAddr()),
			},
		},
	})
	if err := bootstrapFuture.Error(); err != nil {
		logrus.Fatal(err)
	}
}

func getTransport(bindAddr string) (*raft.NetworkTransport, error) {
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	return raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
}

func AddVoter(otherAddr string) {
	otherAddr = fmt.Sprintf("%s:7000", otherAddr)

	config2 := raft.DefaultConfig()
	config2.LocalID = raft.ServerID(otherAddr)
	if raftNode.State() != raft.Leader {
		return
	}

	addVoterFuture := raftNode.AddVoter(config2.LocalID, raft.ServerAddress(otherAddr), 0, 0)
	if err := addVoterFuture.Error(); err != nil {
		logrus.Errorf("AddVoter state: %s error: %v", raftNode.State(), err)
	} else {
		logrus.Warnf("ADD SERVER SUCCESS %s", otherAddr)
	}
}

func RemoveServer(otherAddr string) {
	removeServerFuture := raftNode.RemoveServer(raft.ServerID(otherAddr), 0, 0)
	if raftNode.State() != raft.Leader {
		return
	}

	if err := removeServerFuture.Error(); err != nil {
		logrus.Errorf("RemoveServer state: %s error: %v", raftNode.State(), err)
	} else {
		logrus.Warnf("REMOVE SERVER SUCCESS %s", otherAddr)
	}
}

type StateMachineObserver struct{}

func (o *StateMachineObserver) ApplyLog(logEntry *raft.Log) interface{} {
	// This method is called when a log entry is applied to the state machine
	// You can fire an event here or perform any necessary actions
	fmt.Println("Log applied to the state machine:", logEntry.Data)
	return nil
}

func (o *StateMachineObserver) Snapshot() {}
