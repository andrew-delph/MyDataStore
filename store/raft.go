package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/sirupsen/logrus"
)

var raftNode *raft.Raft

var trans *raft.NetworkTransport

var applyLock sync.RWMutex

var snapshotLock sync.RWMutex

type FSM struct {
	Epoch int64
}

var validFSMObserver = make(chan bool, 1)

var epochObserver = make(chan int64, 10000) // TODO: dont want to miss any updates right now. come up with better solution.

func UpdateEpoch() error {
	logrus.Warnf("Leader Update Epoch. Epoch = %d", globalEpoch+1)

	epochBytes, err := EncodeInt64ToBytes(globalEpoch + 1)
	if err != nil {
		logrus.Errorf("EncodeInt64ToBytes Err= %v", err)
		return err
	}
	logEntry := raftNode.Apply(epochBytes, 0)
	err = logEntry.Error()

	if err == nil {
		// logrus.Warnf("Leader Update Epoch. Epoch = %d", currEpoch)
	} else {
		logrus.Warnf("update fsm Err= %v", err)
	}
	logrus.Debugf("Done.")
	return err
}

func (fsm *FSM) Apply(logEntry *raft.Log) interface{} {
	applyLock.Lock()
	defer applyLock.Unlock()
	epoch, err := DecodeBytesToInt64(logEntry.Data)
	if err != nil {
		logrus.Error("DecodeBytesToInt64 Error on Apply: %v", err)
		return nil
	}
	fsm.Epoch = epoch

	logrus.Debugf("E= %d state= %s name= %s", epoch, raftNode.State(), "TODO FIX.")

	if logEntry.Index == raftNode.AppliedIndex() {
		validFSMObserver <- true
	} else if logEntry.Index != raftNode.AppliedIndex() {
		validFSMObserver <- false
	}

	epochObserver <- epoch

	return epoch
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// logrus.Warnf("Snapshot start")
	// defer logrus.Warnf("Snapshot done")

	snapshotLock.Lock()
	defer snapshotLock.Unlock()

	return &FSMSnapshot{stateValue: fsm.Epoch}, nil
}

func (fsm *FSM) Restore(serialized io.ReadCloser) error {
	var snapshot FSMSnapshot
	if err := json.NewDecoder(serialized).Decode(&snapshot); err != nil {
		return err
	}

	fsm.Epoch = snapshot.stateValue

	return serialized.Close()
}

type FSMSnapshot struct {
	stateValue int64 `json:"value"`
}

func (fsmSnapshot *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// logrus.Warn("Persist!!!!!!!!!!!!!!!!!!!!!!!!1")
	err := func() error {
		snapshotBytes, err := json.Marshal(fsmSnapshot)
		if err != nil {
			return err
		}

		if _, err := sink.Write(snapshotBytes); err != nil {
			return err
		}

		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		logrus.Warnf("Persist ERROR = %v", err)
		sink.Cancel()
		return err
	}

	return nil
}

func (fsmSnapshot *FSMSnapshot) Release() {
}

var raftConf *raft.Config

var (
	logStore      *raftboltdb.BoltStore
	stableStore   *raftboltdb.BoltStore
	snapshotStore *raft.FileSnapshotStore
)

func CreateRaftConf(raftConfig RaftConfig) *raft.Config {
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(theManager.Config.Manager.Hostname)
	// conf.SnapshotInterval = time.Second * 1
	// conf.SnapshotThreshold = 1
	logrus.Infof("conf.ElectionTimeout %v", conf.ElectionTimeout)
	logrus.Infof("conf.HeartbeatTimeout %v", conf.HeartbeatTimeout)
	logrus.Infof("conf.LeaderLeaseTimeout %v", conf.LeaderLeaseTimeout)
	logrus.Infof("conf.CommitTimeout %v", conf.CommitTimeout)
	logrus.Infof("conf.SnapshotInterval %v", conf.SnapshotInterval)
	logrus.Infof("conf.SnapshotThreshold %v", conf.SnapshotThreshold)

	if !raftConfig.EnableLogs {
		raftLogger := hclog.New(&hclog.LoggerOptions{
			Name:   "discard",
			Output: io.Discard,
			Level:  hclog.NoLevel,
		})
		conf.Logger = raftLogger
		conf.LogLevel = "ERROR"
	}
	return conf
}

func SetupRaft(raftConfig RaftConfig) {
	hostname, exists := os.LookupEnv("HOSTNAME")
	if !exists {
		logrus.Fatal("hostname env not set.")
	} else {
		logrus.Infof("hostname='%s'", hostname)
	}

	raftDir = fmt.Sprintf("%s/%s", raftConfig.DataPath, hostname)

	// Create the 'raft-data' directory if it doesn't exist
	if err := os.MkdirAll(raftDir, 0o700); err != nil {
		logrus.Fatal(err)
	}

	raftBindAddr = fmt.Sprintf("%s:7000", hostname)
	advertiseAddr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a store to persist Raft logrus entries
	logStore, err = raftboltdb.NewBoltStore(filepath.Join(raftDir, "log.db"))
	if err != nil {
		logrus.Fatal(err)
	}

	stableStore, err = raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.db"))
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a snapshot store
	snapshotStore, err = raft.NewFileSnapshotStore(filepath.Join(raftDir, "snap"), 2, io.Discard)
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a configuration for raftNode1
	raftConf = CreateRaftConf(raftConfig)

	if trans != nil {
		err = trans.Close()
		if err != nil {
			logrus.Fatal(err)
		}
	}
	trans, err = raft.NewTCPTransport(raftBindAddr, advertiseAddr, 20, 0, os.Stderr)
	if err != nil {
		logrus.Fatal(err)
	}

	// tm := transport.New(raft.ServerAddress(raftBindAddr), []grpc.DialOption{grpc.WithInsecure()})

	fsm := &FSM{} // Your state machine instance
	// Create raftNode
	raftNode, err = raft.NewRaft(raftConf, fsm, logStore, logStore, snapshotStore, trans)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Infof("after SetupRaft state = %s", raftNode.State())
	if raftConfig.AutoBootstrap {
		go func() {
			time.Sleep(time.Duration(raftConfig.BootstrapTimeout))
			if (raftNode.State() != raft.Leader && raftNode.State() != raft.Follower) || raftNode.Leader() == "" {
				err := RaftTryLead()
				if err != nil {
					logrus.Errorf("RaftTryLead timeout err = %v", err)
				}
			} else {
				return
			}
		}()
	}
}

func RaftBootstrap() error {
	bootstrapFuture := raftNode.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raftConf.LocalID,
				Address:  raft.ServerAddress(trans.LocalAddr()),
			},
		},
	})
	return bootstrapFuture.Error()
}

func RaftTryLead() error {
	err := RaftBootstrap()
	if err != nil {
		logrus.Errorf("RaftBootstrap err = %v", err)
		return err
	}
	logrus.Warn("BOOTSTRAP SUCCESS")
	succ := ""
	for i, node := range theManager.gossipCluster.cluster.Members() {
		// logrus.Warnf("node name = %s addr = %s", node.Name, node.Addr.String())
		// go func(node *memberlist.Node) {
		err := AddVoter(node)
		if err != nil {
			logrus.Errorf("RaftTryLead AddVoter: %v", err)
			return nil
		} else {
			logrus.Debugf("%s,%d", succ, i)
		}
		// }(node)
	}
	return nil
}

// var raftLock sync.Mutex

func AddVoter(node *memberlist.Node) error {
	err := raftNode.VerifyLeader().Error()
	if err != nil {
		return nil
	}

	otherRaftAddr := fmt.Sprintf("%s:7000", node.Addr)
	logrus.Debugf("AddVoter otherRaftAddr %s", otherRaftAddr)

	addVoterFuture := raftNode.AddVoter(raft.ServerID(node.Name), raft.ServerAddress(otherRaftAddr), 0, 0)
	return addVoterFuture.Error()
}

func RemoveServer(node *memberlist.Node) error {
	err := raftNode.VerifyLeader().Error()
	if err != nil {
		return nil
	}

	// otherRaftAddr := fmt.Sprintf("%s:7000", node.Addr)
	// logrus.Warn("RemoveServer otherRaftAddr ", otherRaftAddr)

	// raftNode.GetConfiguration().Index()
	err = raftNode.RemoveServer(raft.ServerID(node.Name), 0, 0).Error()
	if err != nil {
		for i := 0; i < 100; i++ {
			logrus.Warn("RemoveServer ", err)
		}
		return err
	}

	return err
}

func Snapshot() error {
	// if raftNode.LastIndex() != raftNode.AppliedIndex() {
	// 	logrus.Warnf("INDEX NOT EQUAL %d %d currEpoch = %d", raftNode.LastIndex(), raftNode.AppliedIndex(), currEpoch)
	// 	return nil
	// }
	err := raftNode.Snapshot().Error()
	if err != nil {
		logrus.Error("Snapshot Error ", err)
	}
	return err
}

func AddAllMembers() {
	for _, node := range theManager.gossipCluster.cluster.Members() {
		err := AddVoter(node)
		if err != nil {
			logrus.Errorf("LeaderCh AddVoter: %v", err)
			continue
		}
	}
}
