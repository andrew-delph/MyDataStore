package gossip

import (
	"io"
	"log"

	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
)

func gossipTest() {
	logrus.Warn("GOSSIP")
}

type GossipCluster struct {
	gossipConfig     config.GossipConfig
	memberlistConfig *memberlist.Config
	list             *memberlist.Memberlist
	msgCh            chan []byte
	reqCh            chan interface{}
}

type JoinTask struct {
	Name string
	IP   string
}

type LeaveTask struct {
	Name string
}

func CreateGossipCluster(gossipConfig config.GossipConfig, reqCh chan interface{}) *GossipCluster {
	gossipCluster := &GossipCluster{list: new(memberlist.Memberlist)}

	memberlistConfig := memberlist.DefaultLocalConfig()
	memberlistConfig.Logger = log.New(io.Discard, "", 0)
	memberlistConfig.BindPort = 8081
	memberlistConfig.AdvertisePort = 8081
	memberlistConfig.Delegate = &Delegate{}
	memberlistConfig.Events = &EventDelegate{
		reqCh: reqCh,
	}
	memberlistConfig.Name = gossipConfig.Name

	gossipCluster.memberlistConfig = memberlistConfig
	gossipCluster.gossipConfig = gossipConfig
	gossipCluster.reqCh = reqCh
	return gossipCluster
}

func (gossipCluster *GossipCluster) Join() error {
	clusterNodes, err := memberlist.Create(gossipCluster.memberlistConfig)
	if err != nil {
		logrus.Fatal(err)
	}

	n, err := clusterNodes.Join(gossipCluster.gossipConfig.InitMembers)
	gossipCluster.list = clusterNodes
	logrus.Debugf("Join n = %d", n)
	return err
}

func (gossipCluster *GossipCluster) GetMembers() []*memberlist.Node {
	return gossipCluster.list.Members()
}

type Delegate struct{}

// Msg implements memberlist.Delegate interface.
func (d *Delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

// NotifyMsg implements memberlist.Delegate interface.
func (d *Delegate) NotifyMsg(b []byte) {}

// GetBroadcasts implements memberlist.Delegate interface.
func (d *Delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return [][]byte{}
}

// LocalState implements memberlist.Delegate interface.
func (d *Delegate) LocalState(join bool) []byte {
	return []byte{}
}

// MergeRemoteState implements memberlist.Delegate interface.
func (d *Delegate) MergeRemoteState(buf []byte, join bool) {}

type EventDelegate struct {
	reqCh chan interface{}
}

// NotifyJoin is invoked when a node joins.
func (e *EventDelegate) NotifyJoin(node *memberlist.Node) {
	logrus.Debugf("leave %s", node.Name)
	e.reqCh <- JoinTask{Name: node.Name, IP: node.Addr.String()}
}

// NotifyLeave is invoked when a node leaves.
func (e *EventDelegate) NotifyLeave(node *memberlist.Node) {
	logrus.Debugf("leave %s", node.Name)
	e.reqCh <- LeaveTask{Name: node.Name}
}

// NotifyUpdate is invoked when a node is updated.
func (e *EventDelegate) NotifyUpdate(n *memberlist.Node) {
	logrus.Warnf("Node updated: %s\n", n.Name)
}
