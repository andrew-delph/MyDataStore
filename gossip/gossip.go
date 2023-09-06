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
}

type LeaveTask struct {
	Name string
}

func CreateGossipCluster(gossipConfig config.GossipConfig, reqCh chan interface{}) GossipCluster {
	gossipCluster := GossipCluster{}

	memberlistConfig := memberlist.DefaultLocalConfig()
	memberlistConfig.Logger = log.New(io.Discard, "", 0)
	memberlistConfig.BindPort = 8081
	memberlistConfig.AdvertisePort = 8081
	memberlistConfig.Delegate = &gossipCluster
	memberlistConfig.Events = &gossipCluster
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

	gossipCluster.list = clusterNodes
	n, err := clusterNodes.Join(gossipCluster.gossipConfig.InitMembers)
	logrus.Debugf("Join n = %d", n)
	return err
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
	return []byte("")
}

func (gossipCluster *GossipCluster) GetBroadcasts(overhead, limit int) [][]byte {
	logrus.Debugf("GetBroadcasts %d %d", overhead, limit)
	return nil
}

func (gossipCluster *GossipCluster) MergeRemoteState(buf []byte, join bool) {
	logrus.Warn("MergeRemoteState")
}

func (gossipCluster *GossipCluster) NotifyJoin(node *memberlist.Node) {
	logrus.Warnf("join %s", node.Name)
	gossipCluster.reqCh <- JoinTask{Name: node.Name}
}

func (gossipCluster *GossipCluster) NotifyLeave(node *memberlist.Node) {
	logrus.Debugf("leave %s", node.Name)
	gossipCluster.reqCh <- LeaveTask{Name: node.Name}
}

func (gossipCluster *GossipCluster) NotifyUpdate(node *memberlist.Node) {
	logrus.Debugf("update %s", node.Name)
}
