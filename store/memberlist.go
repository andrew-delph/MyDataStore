package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	datap "github.com/andrew-delph/my-key-store/datap"
)

type NodeState struct {
	Health bool
}

type NodeClient struct {
	conn   *grpc.ClientConn
	client *datap.InternalNodeServiceClient
}

type GossipCluster struct {
	conf     *memberlist.Config
	cluster  *memberlist.Memberlist
	hashring *HashRing
	msgCh    chan []byte
	state    *NodeState

	consistent *consistent.Consistent
	nodes      map[string]NodeClient
	mu         sync.RWMutex
}

func CreateMemberList(managerConfig ManagerConfig) (*GossipCluster, error) {
	cluster := GossipCluster{}

	conf := memberlist.DefaultLocalConfig()
	conf.Logger = log.New(io.Discard, "", 0)
	conf.BindPort = 8081
	conf.AdvertisePort = 8081
	conf.Delegate = &cluster
	conf.Events = &cluster
	conf.Name = hostname

	clusterNodes, err := memberlist.Create(conf)
	if err != nil {
		return nil, err
	}

	cluster.cluster = clusterNodes
	cluster.hashring = GetHashRing(managerConfig)
	return &cluster, nil
}

func (d *GossipCluster) UpdateNodeHealth(health bool) error {
	logrus.Debug("UpdateNodeHealth >> ", health)
	d.state.Health = health
	err := d.cluster.UpdateNode(0)
	if err != nil {
		logrus.Errorf("UpdateNode err = %v", err)
	}
	return err
}

func (d *GossipCluster) NotifyMsg(msg []byte) {
	d.msgCh <- msg
}

func (d *GossipCluster) NodeMeta(limit int) []byte {
	data, err := json.Marshal(d.state)
	if err != nil {
		logrus.Errorf("NodeMeta err = %v", err)
		return nil
	}
	logrus.Debug("NodeMeta >> ", d.state.Health)
	return data
}

func (d *GossipCluster) LocalState(join bool) []byte {
	// not use, noop
	return []byte("")
}

func (d *GossipCluster) GetBroadcasts(overhead, limit int) [][]byte {
	// not use, noop
	return nil
}

func (d *GossipCluster) MergeRemoteState(buf []byte, join bool) {
	// not use
}

var partitionVerified = make(map[int]bool)

func (events *GossipCluster) GetNodeClient(nodeName string) (datap.InternalNodeServiceClient, error) {
	events.mu.RLock()
	defer events.mu.RUnlock()
	nodeClient, exists := events.nodes[nodeName]
	if !exists {
		return nil, fmt.Errorf("Client does exist for node = %s", nodeName)
	}
	return *nodeClient.client, nil
}

func (events *GossipCluster) NotifyJoin(node *memberlist.Node) {
	var err error
	logrus.Infof("join %s", node.Name)
	events.mu.Lock()
	defer events.mu.Unlock()

	err = events.HandleNotifyUpdate(node)
	if err != nil {
		logrus.Errorf("NotifyLeave err = %v", err)
		return
	}

	conn, client, err := GetClient(node.Addr.String())
	if err != nil {
		logrus.Fatalf("GetClient err=%v", err)
	}

	events.nodes[node.Name] = NodeClient{conn: conn, client: client}
	err = AddVoter(node)
	if err != nil {
		logrus.Errorf("add voter err = %v", err)
	}
}

func (events *GossipCluster) NotifyLeave(node *memberlist.Node) {
	var err error
	logrus.Infof("leave %s", node.Name)
	events.mu.Lock()
	defer events.mu.Unlock()

	err = events.HandleNotifyUpdate(node)
	if err != nil {
		logrus.Errorf("NotifyLeave err = %v", err)
		return
	}

	events.hashring.RemoveNode(node)

	nodeClient, exists := events.nodes[node.Name]
	if exists {
		err := nodeClient.conn.Close()
		if err != nil {
			logrus.Errorf("NotifyLeave conn.Close() err = %v", err)
		}
	}

	delete(events.nodes, node.Name)

	err = RemoveServer(node)
	if err != nil {
		logrus.Errorf("remove server err = %v", err)
		return
	}
}

func (events *GossipCluster) NotifyUpdate(node *memberlist.Node) {
	err := events.HandleNotifyUpdate(node)
	if err != nil {
		logrus.Errorf("NotifyUpdate err = %v", err)
		return
	}
}

func (events *GossipCluster) HandleNotifyUpdate(node *memberlist.Node) error {
	var otherNode NodeState
	err := json.Unmarshal(node.Meta, &otherNode)
	if err != nil {
		logrus.Errorf("HandleNotifyUpdate error deserializing. err = %v", err)
		return err
	}

	if otherNode.Health {
		events.hashring.AddNode(node)
	} else {
		logrus.Warnf("HandleNotifyUpdate name = %s Health = %v", node.Name, otherNode.Health)
		events.hashring.RemoveNode(node)
	}

	return nil
}
