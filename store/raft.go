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

var (
	logStore      *raftboltdb.BoltStore
	stableStore   *raftboltdb.BoltStore
	snapshotStore *raft.FileSnapshotStore
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

	raftBindAddr = fmt.Sprintf("%s:7000", localIp)

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

	transport, err = getTransport(raftBindAddr)
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a store to persist Raft logrus entries
	if logStore == nil {
		logStore, err = raftboltdb.NewBoltStore(filepath.Join(raftDir, "log.db"))
		if err != nil {
			logrus.Fatal(err)
		}
	}

	if stableStore == nil {
		stableStore, err = raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.db"))
		if err != nil {
			logrus.Fatal(err)
		}
	}

	// Create a snapshot store
	if snapshotStore == nil {
		snapshotStore, err = raft.NewFileSnapshotStore(filepath.Join(raftDir, "snap"), 2, io.Discard)
		if err != nil {
			logrus.Fatal(err)
		}
	}

	// Create a configuration for raftNode1
	raftConf = raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(fmt.Sprintf("%s:7000", localIp))

	raftLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "discard",
		Output: io.Discard,
		Level:  hclog.NoLevel,
	})
	raftConf.Logger = raftLogger
	// raftConf.LogLevel = "ERROR"
	logrus.Warnf("raftConf.SnapshotInterval %v ... %v", raftConf.SnapshotInterval)

	fsm = &StateMachine{} // Your state machine instance
	// Create raftNode
	raftNode, err = raft.NewRaft(raftConf, fsm, logStore, stableStore, snapshotStore, transport)
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
		logrus.Errorf("bootstrapFuture %v", err)
	}

	logrus.Warnf("after SetupRaft state = %s", raftNode.State())
}

func getTransport(bindAddr string) (*raft.NetworkTransport, error) {
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	return raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
}

// var raftLock sync.Mutex

func AddVoter(otherAddr string) error {
	if raftNode.State() != raft.Leader {
		return fmt.Errorf("not the leader!")
	}
	err := raftNode.VerifyLeader().Error()
	if err != nil {
		return err
	}

	// raftLock.Lock()         // Lock the critical section
	// defer raftLock.Unlock() // Ensure the lock is released once the function completes

	otherAddr = fmt.Sprintf("%s:7000", otherAddr)

	config2 := raft.DefaultConfig()
	config2.LocalID = raft.ServerID(otherAddr)

	addVoterFuture := raftNode.AddVoter(config2.LocalID, raft.ServerAddress(otherAddr), 0, time.Second)
	return addVoterFuture.Error()
}

func RemoveServer(otherAddr string) {
	if raftNode.State() != raft.Leader {
		return
	}

	// raftLock.Lock()         // Lock the critical section
	// defer raftLock.Unlock() // Ensure the lock is released once the function completes

	removeServerFuture := raftNode.RemoveServer(raft.ServerID(otherAddr), 0, time.Second)

	if err := removeServerFuture.Error(); err != nil {
		logrus.Errorf("RemoveServer state: %s error: %v", raftNode.State(), err)
	} else {
		logrus.Warnf("REMOVE SERVER SUCCESS %s", otherAddr)
	}
}
