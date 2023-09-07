package main

import (
	"context"
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
)

var numWorkers = 1000

func mainTest() {
	logrus.Warn("hi")
}

type Manager struct {
	reqCh            chan interface{}
	db               storage.Storage
	httpServer       *http.HttpServer
	gossipCluster    *gossip.GossipCluster
	consensusCluster *consensus.ConsensusCluster
	ring             *hashring.Hashring
	rpcWrapper       *rpc.RpcWrapper
}

func NewManager() Manager {
	// c := config.GetConfig()
	c := config.GetDefaultConfig()
	reqCh := make(chan interface{}, 100)
	httpServer := http.CreateHttpServer(reqCh)
	gossipCluster := gossip.CreateGossipCluster(c.Gossip, reqCh)
	db := storage.NewBadgerStorage(c.Storage)
	consensusCluster := consensus.CreateConsensusCluster(c.Consensus, reqCh)
	ring := hashring.CreateHashring(c.Hashring)

	rpcWrapper := rpc.CreateRpcWrapper(c.Rpc, reqCh)

	return Manager{reqCh: reqCh, db: db, httpServer: &httpServer, gossipCluster: &gossipCluster, consensusCluster: &consensusCluster, ring: &ring, rpcWrapper: &rpcWrapper}
}

func (m Manager) StartManager() {
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

func (m Manager) startWorkers() {
	for i := 0; i < numWorkers; i++ {
		go m.startWorker()
	}
}

func (m Manager) startWorker() {
	logrus.Debug("starting worker")
	defer logrus.Warn("ending worker")

	for {
		select {
		case data, ok := <-m.reqCh:
			if !ok {
				logrus.Fatal("Channel closed!")
				return
			}

			switch task := data.(type) {
			case http.SetTask:
				logrus.Debugf("worker SetTask: %+v", task)
				err := m.SetRequest(task.Key, task.Value)
				task.ResCh <- err

			case http.GetTask:
				logrus.Debugf("worker GetTask: %+v", task)
				value, err := m.GetRequest(task.Key)
				if err != nil {
					task.ResCh <- err
				} else {
					task.ResCh <- value
				}

			case gossip.JoinTask:
				logrus.Warnf("worker JoinTask: %+v", task)

				_, rpcClient, err := m.rpcWrapper.CreateRpcClient(task.IP)
				if err != nil {
					err = errors.Wrap(err, "gossip.JoinTask")
					logrus.Error(err)
					continue
				}

				m.ring.AddNode(CreateMember(task.Name, rpcClient))
				err = m.consensusCluster.AddVoter(task.Name, task.IP)
				if err != nil {
					err = errors.Wrap(err, "gossip.JoinTask")
					logrus.Error(err)
				} else {
					logrus.Infof("AddVoter success")
				}
			case gossip.LeaveTask:
				logrus.Warnf("worker LeaveTask: %+v", task)
				m.ring.RemoveNode(task.Name)
				m.consensusCluster.RemoveServer(task.Name)
			case consensus.EpochTask:
				logrus.Infof("E = %d", task.Epoch)
			case consensus.LeaderChangeTask:
				if !task.IsLeader {
					continue
				}
				logrus.Infof("worker LeaderChangeTask: %+v", task)

				for _, member := range m.gossipCluster.GetMembers() {
					err := m.consensusCluster.AddVoter(member.Name, member.Addr.String())
					if err != nil {
						err = errors.Wrap(err, "gossip.JoinTask")
						logrus.Error(err)
					} else {
						logrus.Infof("AddVoter success")
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
				logrus.Fatalf("worker unkown task type: %v", reflect.TypeOf(task))
			}
		}
	}
}

func (m Manager) SetRequest(key, value string) error {
	nodes, err := m.ring.GetClosestN(key, 1)
	if err != nil {
		return err
	}
	node := nodes[0]
	member, ok := node.(Member)
	if !ok {
		return errors.New("failed to decode node")
	}
	unixTimestamp := time.Now().Unix()
	setValue := &rpc.RpcValue{Key: key, Value: value, Epoch: int64(1), UnixTimestamp: unixTimestamp}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = member.rpcClient.SetRequest(ctx, setValue)
	return err
}

func (m Manager) GetRequest(key string) (string, error) {
	nodes, err := m.ring.GetClosestN(key, 1)
	if err != nil {
		return "", err
	}
	node := nodes[0]
	member, ok := node.(Member)
	if !ok {
		return "", errors.New("failed to decode node")
	}
	getMessage := &rpc.RpcGetRequestMessage{Key: key}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := member.rpcClient.GetRequest(ctx, getMessage)
	if err != nil {
		return "", err
	}

	return res.Value, nil
}

func main() {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		logrus.Warnf("Recovered from panic: %+v", r)
	// 	}
	// }()
	logrus.Info("starting")
	manager := NewManager()
	manager.StartManager()
}

type Member struct {
	Name      string
	rpcClient rpc.RpcClient
}

func CreateMember(name string, rpcClient rpc.RpcClient) Member {
	return Member{Name: name, rpcClient: rpcClient}
}

func (m Member) String() string {
	return string(m.Name)
}
