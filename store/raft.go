package main

import (
	"encoding/binary"
	"fmt"
	"io"
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
	return hostname
}

func (sm *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	return sm, nil
}

func (sm *StateMachine) Persist(sink raft.SnapshotSink) error {
	sink.Write(Uint64ToBytes(sm.Epoch))
	return sink.Close()
}

func (sm *StateMachine) Release() {
}

func (sm *StateMachine) Restore(serialized io.ReadCloser) error {
	// Implement restoring state from a snapshot if needed
	data := make([]byte, 8)
	_, err := serialized.Read(data)
	if err != nil {
		logrus.Errorf("Restore error = %v", err)
		return err
	}

	restoreEpoch := binary.BigEndian.Uint64(data)
	logrus.Warnf("Restore DATA ---- %d %d %d", restoreEpoch, sm.Epoch, fsm.Epoch)

	sm.Epoch = restoreEpoch
	fsm.Epoch = restoreEpoch

	return serialized.Close()
}

var (
	fsm      *StateMachine
	raftConf *raft.Config
)

var transport *raft.NetworkTransport

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

	raftDir = fmt.Sprintf("/store/raft/raft_%s", hostname)

	// Clean up the previous state
	// os.RemoveAll(raftDir)

	// Create the 'raft-data' directory if it doesn't exist
	if err := os.MkdirAll(raftDir, 0o700); err != nil {
		logrus.Fatal(err)
	}

	// Create a network transport for raftNode1
	if transport != nil {
		err = transport.Close()
		if err != nil {
			logrus.Fatal(err)
		}
	}

	raftBindAddr = fmt.Sprintf("%s:7000", localIp)
	advertiseAddr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
	if err != nil {
		logrus.Fatal(err)
	}

	transport, err = raft.NewTCPTransport(raftBindAddr, advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a store to persist Raft logrus entries
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "log.db"))
	if err != nil {
		logrus.Fatal(err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.db"))
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(raftDir, "snap"), 2, io.Discard)
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a configuration for raftNode1
	raftConf = raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(fmt.Sprintf("%s:7000", localIp))

	if !raftLogs {
		raftLogger := hclog.New(&hclog.LoggerOptions{
			Name:   "discard",
			Output: io.Discard,
			Level:  hclog.NoLevel,
		})
		raftConf.Logger = raftLogger
		raftConf.LogLevel = "ERROR"
	}

	logrus.Warnf("raftConf.SnapshotInterval %v ... %v", raftConf.SnapshotInterval)

	fsm = &StateMachine{} // Your state machine instance
	// Create raftNode
	raftNode, err = raft.NewRaft(raftConf, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Warnf("after SetupRaft state = %s", raftNode.State())
}

func RaftBootstrap() error {
	bootstrapFuture := raftNode.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raftConf.LocalID,
				Address:  raft.ServerAddress(transport.LocalAddr()),
			},
		},
	})
	return bootstrapFuture.Error()
}

// var raftLock sync.Mutex

func AddVoter(otherAddr string) error {
	err := raftNode.VerifyLeader().Error()
	if err != nil {
		return err
	}

	// raftLock.Lock()         // Lock the critical section
	// defer raftLock.Unlock() // Ensure the lock is released once the function completes

	otherAddr = fmt.Sprintf("%s:7000", otherAddr)

	config2 := raft.DefaultConfig()
	config2.LocalID = raft.ServerID(otherAddr)
	// if raftNode.AppliedIndex() > 20 {
	// 	addVoterFuture := raftNode.AddVoter(config2.LocalID, raft.ServerAddress(otherAddr), raftNode.AppliedIndex(), time.Second)
	// 	return addVoterFuture.Error()
	// } else {
	// 	addVoterFuture := raftNode.AddVoter(config2.LocalID, raft.ServerAddress(otherAddr), 0, time.Second)
	// 	return addVoterFuture.Error()
	// }
	addVoterFuture := raftNode.AddVoter(config2.LocalID, raft.ServerAddress(otherAddr), 0, time.Second)
	return addVoterFuture.Error()
}

func RemoveServer(otherAddr string) error {
	err := raftNode.VerifyLeader().Error()
	if err != nil {
		return err
	}

	// raftLock.Lock()         // Lock the critical section
	// defer raftLock.Unlock() // Ensure the lock is released once the function completes

	removeServerFuture := raftNode.RemoveServer(raft.ServerID(otherAddr), 0, time.Second)

	return removeServerFuture.Error()
}
