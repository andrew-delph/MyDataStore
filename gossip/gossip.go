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
	memberlistConfig.Delegate = gossipCluster
	memberlistConfig.Events = gossipCluster
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

func (gossipCluster *GossipCluster) NotifyMsg(msg []byte) {
	gossipCluster.msgCh <- msg
}

func (gossipCluster *GossipCluster) NodeMeta(limit int) []byte {
	logrus.Debugf("NodeMeta")
	return nil
}

func (gossipCluster *GossipCluster) LocalState(join bool) []byte {
	logrus.Debugf("LocalState")
	return nil
}

func (gossipCluster *GossipCluster) GetBroadcasts(overhead, limit int) [][]byte {
	logrus.Debugf("GetBroadcasts %d %d", overhead, limit)
	return nil
}

func (gossipCluster *GossipCluster) MergeRemoteState(buf []byte, join bool) {
	logrus.Warn("MergeRemoteState")
}

func (gossipCluster *GossipCluster) NotifyJoin(node *memberlist.Node) {
	logrus.Debugf("join %s num = %d %d", node.Name, len(gossipCluster.list.Members()), gossipCluster.list.NumMembers())
	gossipCluster.reqCh <- JoinTask{Name: node.Name, IP: node.Addr.String()}
}

func (gossipCluster *GossipCluster) NotifyLeave(node *memberlist.Node) {
	logrus.Warnf("leave %s", node.Name)
	gossipCluster.reqCh <- LeaveTask{Name: node.Name}
}

func (gossipCluster *GossipCluster) NotifyUpdate(node *memberlist.Node) {
	logrus.Debugf("update %s", node.Name)
}
