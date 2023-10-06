package gossip

import (
	"io"
	"log"
	"net"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/utils"
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

	memberlistConfig := memberlist.DefaultWANConfig()
	if gossipConfig.EnableLogs == false {
		memberlistConfig.Logger = log.New(io.Discard, "", 0)
	}
	memberlistConfig.BindPort = 8081
	memberlistConfig.AdvertisePort = 8081
	memberlistConfig.Delegate = &Delegate{}
	memberlistConfig.Events = &EventDelegate{
		reqCh: reqCh,
	}
	memberlistConfig.Conflict = &ConflictDelegate{
		reqCh: reqCh,
	}
	memberlistConfig.Name = gossipConfig.Name

	// set the AdvertiseAddr
	ipAddresses, err := net.LookupIP(gossipConfig.Name)
	if err != nil {
		logrus.Fatalf("Error resolving IP address for %s: %v\n", gossipConfig.Name, err)
	}
	memberlistConfig.AdvertiseAddr = ipAddresses[0].String()
	logrus.Debugf("AdvertiseAddr = %s", ipAddresses[0].String())

	// behavior configurations
	memberlistConfig.DeadNodeReclaimTime = time.Duration(time.Nanosecond * 1)

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

func (gossipCluster *GossipCluster) Leave() error {
	defer utils.TrackTime(time.Now(), 0, "Gossip Leave")
	return gossipCluster.list.Leave(time.Second * 5)
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

type ConflictDelegate struct {
	reqCh chan interface{}
}

func (c *ConflictDelegate) NotifyConflict(existing, other *memberlist.Node) {
	logrus.Warnf("NotifyConflict: %s %s Addr: %s %s", existing, other, existing.Addr, other.Addr)
	existing.Addr = other.Addr
	c.reqCh <- JoinTask{Name: other.Name, IP: other.Addr.String()}
}
