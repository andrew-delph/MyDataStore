package consensus

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/datap"
)

func consensusTest() {
	logrus.Warn("CONSENSUS")
}

type ConsensusCluster struct {
	raftConf        *raft.Config
	reqCh           chan interface{}
	consensusConfig config.ConsensusConfig
	raftNode        *raft.Raft
	epochTick       *time.Ticker
	fsm             *FSM
	memberLock      sync.Mutex
	epochLock       bool
}

type FsmTask struct {
	Epoch   int64
	Members []string
	ResCh   chan interface{}
}

func CreateConsensusCluster(consensusConfig config.ConsensusConfig, reqCh chan interface{}) *ConsensusCluster {
	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(consensusConfig.Name)
	// raftConf.SnapshotInterval = time.Second * 1
	// raftConf.SnapshotThreshold = 1
	// logrus.Infof("conf.ElectionTimeout %v", raftConf.ElectionTimeout)
	// logrus.Infof("conf.HeartbeatTimeout %v", raftConf.HeartbeatTimeout)
	// logrus.Infof("conf.LeaderLeaseTimeout %v", raftConf.LeaderLeaseTimeout)
	// logrus.Infof("conf.CommitTimeout %v", raftConf.CommitTimeout)
	// logrus.Infof("conf.SnapshotInterval %v", raftConf.SnapshotInterval)
	// logrus.Infof("conf.SnapshotThreshold %v", raftConf.SnapshotThreshold)

	if !consensusConfig.EnableLogs {
		raftLogger := hclog.New(&hclog.LoggerOptions{
			Name:   "discard",
			Output: io.Discard,
			Level:  hclog.NoLevel,
		})
		raftConf.Logger = raftLogger
		raftConf.LogLevel = "ERROR"
	}

	return &ConsensusCluster{consensusConfig: consensusConfig, reqCh: reqCh, raftConf: raftConf, raftNode: new(raft.Raft), epochTick: new(time.Ticker)}
}

func (consensusCluster *ConsensusCluster) LockEpoch() {
	consensusCluster.epochLock = true
}

func (consensusCluster *ConsensusCluster) UnlockEpoch() {
	consensusCluster.epochLock = false
}

func (consensusCluster *ConsensusCluster) StartConsensusCluster() error {
	logrus.Debug("StartConsensusCluster")

	consensusCluster.epochTick = time.NewTicker(time.Duration(consensusCluster.consensusConfig.EpochTime) * time.Second)

	raftDir := fmt.Sprintf("%s/%s", consensusCluster.consensusConfig.DataPath, consensusCluster.consensusConfig.Name)
	// Create the 'raft-data' directory if it doesn't exist
	if err := os.MkdirAll(raftDir, 0o700); err != nil {
		logrus.Fatal(err)
	}

	raftBindAddr := fmt.Sprintf("%s:7000", consensusCluster.consensusConfig.Name)
	advertiseAddr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
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

	trans, err := raft.NewTCPTransport(raftBindAddr, advertiseAddr, 20, 0, os.Stderr)
	if err != nil {
		logrus.Fatal(err)
	}

	fsm := &FSM{reqCh: consensusCluster.reqCh, index: new(uint64), data: &datap.Fsm{}}

	raftNode, err := raft.NewRaft(consensusCluster.raftConf, fsm, logStore, stableStore, snapshotStore, trans)
	if err != nil {
		logrus.Fatal(err)
	}
	consensusCluster.fsm = fsm
	consensusCluster.raftNode = raftNode

	// start the consensus worker

	if consensusCluster.consensusConfig.AutoBootstrap {
		go func() {
			time.Sleep(time.Duration(consensusCluster.consensusConfig.BootstrapTimeout) * time.Second)
			if (raftNode.State() != raft.Leader && raftNode.State() != raft.Follower) || raftNode.Leader() == "" {
				err := consensusCluster.RaftBootstrap()
				if err != nil {
					logrus.Errorf("RaftBootstrap timeout err = %v", err)
				}
			} else {
				return
			}
		}()
	}

	return nil
}

func (consensusCluster *ConsensusCluster) RaftBootstrap() error {
	// TODO dont create the advertiseAddr again...
	raftBindAddr := fmt.Sprintf("%s:7000", consensusCluster.consensusConfig.Name)
	advertiseAddr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
	if err != nil {
		logrus.Fatal(err)
	}
	bootstrapFuture := consensusCluster.raftNode.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       consensusCluster.raftConf.LocalID,
				Address:  raft.ServerAddress(advertiseAddr.String()),
			},
		},
	})
	return bootstrapFuture.Error()
}

func (consensusCluster *ConsensusCluster) LeaderCh() <-chan bool {
	return consensusCluster.raftNode.LeaderCh()
}

func (consensusCluster *ConsensusCluster) AddVoter(nodeName, nodeIP string) error {
	if consensusCluster.raftNode.State() != raft.Leader {
		return nil
	}

	err := consensusCluster.raftNode.VerifyLeader().Error()
	if err != nil {
		return err
	}
	consensusCluster.memberLock.Lock()
	defer consensusCluster.memberLock.Unlock()

	noderRaftAddr := fmt.Sprintf("%s:7000", nodeIP)
	logrus.Debugf("AddVoter! STATE = %s nodeName = %s noderRaftAddr = %s", consensusCluster.raftNode.State(), nodeName, noderRaftAddr)

	serverId := raft.ServerID(nodeName)
	serverAddress := raft.ServerAddress(noderRaftAddr)

	configFuture := consensusCluster.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		logrus.Errorf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == serverId || srv.Address == serverAddress {
			// However, if *both* the ID and the address are the same, then no
			// join is actually needed.
			if srv.Address == serverAddress && srv.ID == serverId {
				logrus.Debugf("node %s at %s already member of cluster, ignoring join request", nodeName, nodeIP)
				return nil
			}
			if err := consensusCluster.RemoveServer(nodeName); err != nil {
				// if err := consensusCluster.raftNode.RemoveServer(serverId, 0, 0).Error(); err != nil {
				logrus.Errorf("failed to remove node %s: %v", nodeName, err)
				return err
			}
			logrus.Debugf("removed node %s prior to rejoin with changed ID or address", nodeName)
		}
	}

	addVoterFuture := consensusCluster.raftNode.AddVoter(serverId, serverAddress, 0, 0)
	err = addVoterFuture.Error()
	if err != nil {
		logrus.Errorf("retry AddVoter: %v", err)
		return consensusCluster.AddVoter(nodeName, nodeIP)
	}
	// logrus.Warnf("AddVoter Success: %s", nodeName)
	return nil
}

func (consensusCluster *ConsensusCluster) RemoveServer(nodeName string) error {
	err := consensusCluster.raftNode.VerifyLeader().Error()
	if err != nil {
		return nil
	}

	return consensusCluster.raftNode.RemoveServer(raft.ServerID(nodeName), 0, 0).Error()
}

func (consensusCluster *ConsensusCluster) State() raft.RaftState {
	return consensusCluster.raftNode.State()
}

func (consensusCluster *ConsensusCluster) UpdateEpoch() error {
	err := consensusCluster.raftNode.VerifyLeader().Error()
	if err != nil {
		return nil
	}

	if consensusCluster.epochLock {
		logrus.Warn("epoch is currently locked")
	}

	cloneBytes, err := proto.Marshal(consensusCluster.fsm.data)
	if err != nil {
		return err
	}

	clone := &datap.Fsm{}
	err = proto.Unmarshal(cloneBytes, clone)
	if err != nil {
		return err
	}

	clone.Epoch = clone.Epoch + 1

	updateBytes, err := proto.Marshal(clone)
	if err != nil {
		return err
	}

	logEntry := consensusCluster.raftNode.Apply(updateBytes, 0)
	err = logEntry.Error()

	if err == nil {
		logrus.Warnf("Leader Updated Epoch")
	} else {
		logrus.Errorf("update fsm Err= %v", err)
	}
	logrus.Debugf("Done.")
	return err
}

func (consensusCluster *ConsensusCluster) UpdateMembers(Epoch int64, members []string) error {
	err := consensusCluster.raftNode.VerifyLeader().Error()
	if err != nil {
		return nil
	}

	fsmUpdate := &datap.Fsm{Epoch: Epoch, Members: members}

	updateBytes, err := proto.Marshal(fsmUpdate)
	if err != nil {
		return err
	}

	logEntry := consensusCluster.raftNode.Apply(updateBytes, 0)
	err = logEntry.Error()

	if err == nil {
		logrus.Warnf("Leader Updated Members")
	} else {
		logrus.Errorf("update fsm Err= %v", err)
	}
	logrus.Debugf("Done.")
	return err
}

func (consensusCluster *ConsensusCluster) IsHealthy() error {
	currState := consensusCluster.raftNode.State()
	currEpoch := consensusCluster.fsm.data.Epoch
	currLeader := consensusCluster.raftNode.Leader()

	appliedIndex := consensusCluster.raftNode.AppliedIndex()
	fsmIndex := *consensusCluster.fsm.index
	if fsmIndex != appliedIndex {
		logrus.Debugf("fsm wrong index fsmIndex %v appliedIndex %d", fsmIndex, appliedIndex)
	}

	if currState != raft.Follower && currState != raft.Leader {
		return errors.Errorf("wrong state %v. currEpoch: %d currLeader: %s", currState, currEpoch, currLeader)
	}

	return nil
}

func (consensusCluster *ConsensusCluster) Details() error {
	logrus.Warnf("num peers: %v state: %s", consensusCluster.raftNode.Stats()["num_peers"], consensusCluster.raftNode.State())

	return nil
}

func (consensusCluster *ConsensusCluster) Snapshot() error {
	return consensusCluster.raftNode.Snapshot().Error()
}

func (consensusCluster *ConsensusCluster) Shutdown() error {
	err := consensusCluster.raftNode.VerifyLeader().Error()
	if err != nil {
		return nil
	}
	return consensusCluster.raftNode.LeadershipTransfer().Error()
}
