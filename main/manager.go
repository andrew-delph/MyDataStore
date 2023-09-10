package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/consensus"
	"github.com/andrew-delph/my-key-store/gossip"
	"github.com/andrew-delph/my-key-store/hashring"
	"github.com/andrew-delph/my-key-store/http"
	"github.com/andrew-delph/my-key-store/rpc"
	"github.com/andrew-delph/my-key-store/storage"
	"github.com/andrew-delph/my-key-store/utils"
)

type Manager struct {
	config                config.Config
	reqCh                 chan interface{}
	db                    storage.Storage
	httpServer            *http.HttpServer
	gossipCluster         *gossip.GossipCluster
	consensusCluster      *consensus.ConsensusCluster
	ring                  *hashring.Hashring
	rpcWrapper            *rpc.RpcWrapper
	myPartitions          *utils.IntSet
	partitionLocker       *PartitionLocker
	consistencyController *ConsistencyController
	debugTick             *time.Ticker
}

func NewManager() Manager {
	c := config.GetDefaultConfig()
	reqCh := make(chan interface{}, c.Manager.ReqChannelSize)
	httpServer := http.CreateHttpServer(c.Http, reqCh)
	gossipCluster := gossip.CreateGossipCluster(c.Gossip, reqCh)
	db := storage.NewBadgerStorage(c.Storage)
	consensusCluster := consensus.CreateConsensusCluster(c.Consensus, reqCh)
	ring := hashring.CreateHashring(c.Manager)

	rpcWrapper := rpc.CreateRpcWrapper(c.Rpc, reqCh)
	parts := utils.NewIntSet()
	partitionLocker := NewPartitionLocker(c.Manager.PartitionCount)

	consistencyController := NewConsistencyController(c.Manager.PartitionCount)
	return Manager{
		config:                c,
		reqCh:                 reqCh,
		db:                    db,
		httpServer:            &httpServer,
		gossipCluster:         gossipCluster,
		consensusCluster:      consensusCluster,
		ring:                  ring,
		rpcWrapper:            rpcWrapper,
		myPartitions:          &parts,
		partitionLocker:       partitionLocker,
		consistencyController: consistencyController,
		debugTick:             time.NewTicker(time.Second * 100000),
	}
}

func (m *Manager) StartManager() {
	var err error
	go m.startWorkers()

	err = m.consensusCluster.StartConsensusCluster()
	if err != nil {
		logrus.Fatal(err)
	}
	err = m.gossipCluster.Join()
	if err != nil {
		logrus.Fatal(err)
	}

	go m.rpcWrapper.StartRpcServer()
	go m.httpServer.StartHttp()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	<-signals
	logrus.Warn("Received SIGTERM signal")
}

func (m *Manager) startWorkers() {
	for i := 0; i < m.config.Manager.WokersCount; i++ {
		go m.startWorker()
	}
}

func (m *Manager) startWorker() {
	logrus.Debug("starting worker")
	defer logrus.Warn("ending worker")

	for {
		select {
		case <-m.debugTick.C:
			logrus.Warnf("DEBUG TICK #members = %d", len(m.gossipCluster.GetMembers()))
		case data, ok := <-m.reqCh:
			if !ok {
				logrus.Fatal("Channel closed!")
				return
			}
			switch task := data.(type) {
			case http.SetTask:
				logrus.Debugf("worker SetTask: %+v", task)
				err := m.SetRequest(task.Key, task.Value)
				if err != nil {
					task.ResCh <- err
				} else {
					task.ResCh <- "value set"
				}

			case http.GetTask:
				logrus.Debugf("worker GetTask: %+v", task)
				value, err := m.GetRequest(task.Key)
				if err != nil {
					task.ResCh <- err
				} else {
					task.ResCh <- value
				}

			case gossip.JoinTask:
				// logrus.Warnf("worker JoinTask: %+v", task)
				_, rpcClient, err := m.rpcWrapper.CreateRpcClient(task.IP)
				if err != nil {
					err = errors.Wrap(err, "gossip.JoinTask")
					logrus.Fatal(err)
					continue
				}

				m.ring.AddNode(CreateRingMember(task.Name, rpcClient))
				m.HandleHashringChange()
				err = m.consensusCluster.AddVoter(task.Name, task.IP)
				if err != nil {
					err = errors.Wrap(err, "gossip.JoinTask")
					// logrus.Error(err)
				} else {
					// logrus.Infof("AddVoter success")
				}
			case gossip.LeaveTask:
				// logrus.Warnf("worker LeaveTask: %+v", task)
				m.ring.RemoveNode(task.Name)
				m.HandleHashringChange()
				m.consensusCluster.RemoveServer(task.Name)
			case consensus.EpochTask:
				m.VerifyEpoch(task.Epoch)
			case consensus.LeaderChangeTask:
				if !task.IsLeader {
					continue
				}
				logrus.Debugf("worker LeaderChangeTask: %+v", task)

				for _, member := range m.gossipCluster.GetMembers() {
					err := m.consensusCluster.AddVoter(member.Name, member.Addr.String())
					if err != nil {
						err = errors.Wrap(err, "gossip.JoinTask")
						logrus.Debug(err)
						break
					} else {
						logrus.Debugf("AddVoter success")
					}
				}

			case rpc.SetValueTask:
				logrus.Debugf("worker SetValueTask: %+v", task)
				err := m.db.Put([]byte(task.Key), []byte(task.Value))
				if err != nil {
					task.ResCh <- err
				} else {
					task.ResCh <- true
				}

			case rpc.GetValueTask:
				logrus.Debugf("worker GetValueTask: %+v", task)
				value, err := m.db.Get([]byte(task.Key))
				if err != nil {
					task.ResCh <- err
				} else {
					task.ResCh <- value
				}

			case rpc.GetPartitionEpochObjectTask:
				logrus.Warnf("worker GetPartitionEpochObjectTask: %+v", task)
				panic("unimplemented")

			default:
				logrus.Panicf("worker unkown task type: %v", reflect.TypeOf(task))
			}
		}
	}
}

func (m *Manager) SetRequest(key, value string) error {
	nodes, err := m.ring.GetClosestN(key, m.config.Manager.ReplicaCount)
	if err != nil {
		return err
	}

	unixTimestamp := time.Now().Unix()
	setValue := &rpc.RpcValue{Key: key, Value: value, Epoch: int64(1), UnixTimestamp: unixTimestamp}

	responseCh := make(chan *rpc.RpcStandardResponse, m.config.Manager.ReplicaCount)
	errorCh := make(chan error, m.config.Manager.ReplicaCount)

	for _, node := range nodes {
		member, ok := node.(RingMember)
		if !ok {
			return errors.New("failed to decode node")
		}

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := member.rpcClient.SetRequest(ctx, setValue)
			if err != nil {
				errorCh <- err
			} else {
				responseCh <- res
			}
		}()
	}

	timeout := time.After(time.Second * time.Duration(m.config.Manager.DefaultTimeout))
	responseCount := 0

	for i := 0; i < m.config.Manager.ReplicaCount && responseCount < m.config.Manager.WriteQuorum; i++ {
		select {
		case <-responseCh:
			responseCount++
		case err := <-errorCh:
			logrus.Errorf("errorCh: %v", err)
			_ = err // Handle error if necessary
		case <-timeout:
			return fmt.Errorf("timed out waiting for responses. responseCount = %d", responseCount)
		}
	}
	if responseCount < m.config.Manager.WriteQuorum {
		return fmt.Errorf("failed WriteQuorum. responseCount = %d", responseCount)
	} else {
		return nil
	}
}

func (m *Manager) GetRequest(key string) (string, error) {
	nodes, err := m.ring.GetClosestN(key, m.config.Manager.ReplicaCount)
	if err != nil {
		return "", err
	}

	getMessage := &rpc.RpcGetRequestMessage{Key: key}
	responseCh := make(chan *rpc.RpcValue, m.config.Manager.ReplicaCount)
	errorCh := make(chan error, m.config.Manager.ReplicaCount)

	for _, node := range nodes {
		member, ok := node.(RingMember)
		if !ok {
			return "", errors.New("failed to decode node")
		}

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := member.rpcClient.GetRequest(ctx, getMessage)
			if err != nil {
				errorCh <- err
			} else {
				responseCh <- res
			}
		}()
	}

	responseCount := 0
	var recentValue *rpc.RpcValue
	timeout := time.After(time.Second * time.Duration(m.config.Manager.DefaultTimeout))
	for i := 0; i < m.config.Manager.ReplicaCount && responseCount < m.config.Manager.ReadQuorum; i++ {
		select {
		case res := <-responseCh:
			responseCount++

			if recentValue == nil {
				recentValue = res
			} else if recentValue.Epoch <= res.Epoch && recentValue.UnixTimestamp < res.UnixTimestamp {
				recentValue = res
			}
		case err := <-errorCh:
			logrus.Debugf("GET ERROR = %v", err)

		case <-timeout:
			return "", fmt.Errorf("timed out waiting for responses. responseCount = %d", responseCount)
		}
	}
	if recentValue == nil {
		return "", fmt.Errorf("value not found. responseCount = %d", responseCount)
	} else if responseCount < m.config.Manager.ReadQuorum {
		return "", fmt.Errorf("failed ReadQuorum. responseCount = %d", responseCount)
	} else {
		return recentValue.Value, nil
	}
}
