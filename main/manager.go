package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

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
	CurrentEpoch          int64
}

func NewManager(c config.Config) Manager {
	reqCh := make(chan interface{}, c.Manager.ReqChannelSize)
	httpServer := http.CreateHttpServer(c.Http, reqCh)
	gossipCluster := gossip.CreateGossipCluster(c.Gossip, reqCh)
	db := storage.NewBadgerStorage(c.Storage)
	consensusCluster := consensus.CreateConsensusCluster(c.Consensus, reqCh)
	ring := hashring.CreateHashring(c.Manager)

	rpcWrapper := rpc.CreateRpcWrapper(c.Rpc, reqCh)
	parts := utils.NewIntSet()
	partitionLocker := NewPartitionLocker(c.Manager.PartitionCount)

	consistencyController := NewConsistencyController(c.Manager.PartitionCount, reqCh)
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
	if m.config.Manager.PartitionBuckets%2 != 0 {
		logrus.Fatalf("PartitionBuckets must be even. PartitionBuckets = %d", m.config.Manager.PartitionBuckets)
	}
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
		go m.startWorker(i)
	}
}

func (m *Manager) startWorker(workerId int) {
	logrus.Debugf("starting worker %d", workerId)
	defer logrus.Panicf("ending worker %d", workerId)

	for {
	workerLoop:
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

				currPartitionsList, err := m.ring.GetMyPartions()
				if err != nil {
					logrus.Error(err)
					continue
				}
				currPartitions := utils.NewIntSet().From(currPartitionsList)
				m.consistencyController.HandleHashringChange(currPartitions)

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

				currPartitionsList, err := m.ring.GetMyPartions()
				if err != nil {
					logrus.Error(err)
					continue
				}
				currPartitions := utils.NewIntSet().From(currPartitionsList)
				m.consistencyController.HandleHashringChange(currPartitions)

				m.consensusCluster.RemoveServer(task.Name)

			case consensus.EpochTask:
				m.CurrentEpoch = task.Epoch
				m.consistencyController.VerifyEpoch(task.Epoch)

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
				err := m.SetValue(task.Value)
				if err != nil {
					task.ResCh <- err
				} else {
					task.ResCh <- true
				}

			case rpc.GetValueTask:
				logrus.Debugf("worker GetValueTask: %+v", task)
				// value, err := m.db.Get([]byte(task.Key))
				value, err := m.GetValue(task.Key)
				if err != nil {
					task.ResCh <- err
				} else {
					task.ResCh <- value
				}

			case rpc.StreamBucketsTask: // TODO test this is returning right values
				logrus.Warnf("worker StreamBucketsTask: %+v", task)
				var buckets []int32 = task.Buckets
				if len(buckets) == 0 {
					for i := 0; i < m.config.Manager.PartitionBuckets; i++ {
						buckets = append(buckets, int32(i))
					}
				}
				for _, bucket := range buckets {
					index1, err := BuildEpochIndex(int(task.PartitionId), uint64(bucket), task.LowerEpoch, "")
					if err != nil {
						logrus.Fatal(err)
						continue
					}
					index2, err := BuildEpochIndex(int(task.PartitionId), uint64(bucket), task.UpperEpoch, "")
					if err != nil {
						logrus.Fatal(err)
						continue
					}
					it := m.db.NewIterator(
						[]byte(index1),
						[]byte(index2),
						false,
					)
					for !it.IsDone() {
						_, _, epoch, key, err := ParseEpochIndex(string(it.Key()))
						if err != nil {
							logrus.Fatal(err)
							continue
						}
						timestamp, err := utils.DecodeBytesToInt64(it.Value())
						if err != nil {
							logrus.Fatal(err)
							continue
						}

						task.ResCh <- &rpc.RpcValue{Key: key, Epoch: epoch, UnixTimestamp: timestamp}
						it.Next()
					}
					it.Release()
				}

				close(task.ResCh)

			case VerifyPartitionEpochRequestTask:
				logrus.Debugf("worker VerifyPartitionEpochRequestTask: %+v", task.PartitionId)
				// create myTree
				myTree, err := m.RawPartitionMerkleTree(task.PartitionId, task.Epoch, task.Epoch+1)
				if err != nil {
					task.ResCh <- err
					continue
				}
				// serialize
				partitionEpochObject, err := MerkleTreeToPartitionEpochObject(myTree, task.PartitionId, task.Epoch, task.Epoch+1)
				data, err := proto.Marshal(partitionEpochObject)
				if err != nil {
					task.ResCh <- err
					continue
				}

				// save to db
				err = m.db.Put([]byte(BuildEpochTreeObjectIndex(task.PartitionId, task.Epoch)), data)
				if err != nil {
					task.ResCh <- err
					continue
				}

				var epochTreeObjects []*rpc.RpcEpochTreeObject

				epochTreeObjects, err = m.EpochTreeObjectRequest(task.PartitionId, task.Epoch, time.Second*20)
				if err != nil {
					task.ResCh <- err
					continue
				}

				if len(epochTreeObjects) < m.config.Manager.ReadQuorum {
					task.ResCh <- errors.New("did not recieve enough trees")
					continue
				}

				// compare the difference to the otherTree
				validCount := 0
				for _, epochTreeObject := range epochTreeObjects {
					otherTree, err := EpochTreeObjectToMerkleTree(epochTreeObject)
					if err != nil {
						task.ResCh <- err
						break workerLoop
					}
					diff, err := DifferentMerkleTreeBuckets(myTree, otherTree)
					if err != nil {
						task.ResCh <- err
						break workerLoop
					}

					if len(diff) == 0 {
						validCount++
					}
				}

				if validCount >= m.config.Manager.ReadQuorum {
					// TODO update Valid to true
					task.ResCh <- VerifyPartitionEpochResponse{Valid: true}
				} else {
					task.ResCh <- errors.Errorf("did not get enough valid trees. validCount = %d partitionId = %d epoch = %d", validCount, task.PartitionId, task.Epoch)
				}

			case rpc.GetEpochTreeObjectTask:
				logrus.Debugf("worker GetPartitionEpochObjectTask: %+v", task)

				epochTreeObjectBytes, err := m.db.Get([]byte(BuildEpochTreeObjectIndex(int(task.PartitionId), task.LowerEpoch)))
				if err != nil {
					task.ResCh <- err
					continue
				}

				epochTreeObject := &rpc.RpcEpochTreeObject{}
				err = proto.Unmarshal(epochTreeObjectBytes, epochTreeObject)
				if err != nil {
					task.ResCh <- err
					continue
				}
				task.ResCh <- epochTreeObject

			case rpc.GetEpochTreeLastValidObjectTask:
				logrus.Debugf("worker GetEpochTreeLastValidObjectTask: %+v", task)
				// TODO impl
				task.ResCh <- nil

			case SyncPartitionTask:
				logrus.Debugf("worker SyncPartitionTask: %+v", task)
				task.ResCh <- SyncPartitionResponse{Valid: true}

			default:
				logrus.Panicf("worker unkown task type: %v", reflect.TypeOf(task))
				break workerLoop
			}
		}
	}
}

func (m *Manager) SetRequest(key, value string) error {
	nodes, err := m.ring.GetClosestN(key, m.config.Manager.ReplicaCount, true)
	if err != nil {
		return err
	}

	unixTimestamp := time.Now().Unix()
	setReq := &rpc.RpcValue{Key: key, Value: value, Epoch: m.CurrentEpoch, UnixTimestamp: unixTimestamp}

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
			res, err := member.rpcClient.SetRequest(ctx, setReq)
			if err != nil {
				errorCh <- err
			} else {
				responseCh <- res
			}
		}()
		// break
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
	nodes, err := m.ring.GetClosestN(key, m.config.Manager.ReplicaCount, true)
	if err != nil {
		return "", err
	}

	getReq := &rpc.RpcGetRequestMessage{Key: key}
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
			res, err := member.rpcClient.GetRequest(ctx, getReq)
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

func (m *Manager) EpochTreeObjectRequest(partitionId int, epoch int64, timeout time.Duration) ([]*rpc.RpcEpochTreeObject, error) {
	nodes, err := m.ring.GetClosestNForPartition(partitionId, m.config.Manager.ReplicaCount, true)
	if err != nil {
		return nil, err
	}

	treeReq := &rpc.RpcEpochTreeObject{Partition: int32(partitionId), LowerEpoch: epoch, UpperEpoch: epoch + 1}
	responseCh := make(chan *rpc.RpcEpochTreeObject, m.config.Manager.ReplicaCount)
	errorCh := make(chan error, m.config.Manager.ReplicaCount)

	for _, node := range nodes {
		member, ok := node.(RingMember)
		if !ok {
			return nil, errors.New("failed to decode node")
		}

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			res, err := member.rpcClient.GetEpochTree(ctx, treeReq)
			if err != nil {
				errorCh <- err
			} else {
				responseCh <- res
			}
		}()
	}

	var otherTrees []*rpc.RpcEpochTreeObject
	for i := 0; i < len(nodes); i++ {
		select {
		case res := <-responseCh:
			otherTrees = append(otherTrees, res)
		case err := <-errorCh:
			logrus.Debugf("GetEpochTree err = %v", err)
		}
	}
	return otherTrees, nil
}

func (m *Manager) SetValue(value *rpc.RpcValue) error {
	keyBytes := []byte(value.Key)
	timestampBytes, err := utils.EncodeInt64ToBytes(value.UnixTimestamp)
	if err != nil {
		return err
	}
	valueData, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	partitionId := m.ring.FindPartitionID(keyBytes)
	hash := sha256.Sum256(keyBytes)
	bucket := binary.BigEndian.Uint64(hash[:8]) % uint64(m.config.Manager.PartitionBuckets)
	epochIndex, err := BuildEpochIndex(partitionId, bucket, value.Epoch, value.Key)
	if err != nil {
		return err
	}
	keyIndex := BuildKeyIndex(value.Key)
	trx := m.db.NewTransaction(true)
	trx.Set([]byte(keyIndex), valueData)
	trx.Set([]byte(epochIndex), timestampBytes)
	return trx.Commit()
}

func (m *Manager) GetValue(key string) (*rpc.RpcValue, error) {
	keyIndex := BuildKeyIndex(key)
	valueBytes, err := m.db.Get([]byte(keyIndex))
	if err != nil {
		return nil, err
	}
	value := &rpc.RpcValue{}
	err = proto.Unmarshal(valueBytes, value)
	if err != nil {
		return nil, err
	}
	return value, nil
}
