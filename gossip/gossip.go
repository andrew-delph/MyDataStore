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
	conf    *memberlist.Config
	cluster *memberlist.Memberlist
	msgCh   chan []byte
}

func CreateMemberList(managerConfig config.ManagerConfig) (*GossipCluster, error) {
	cluster := GossipCluster{}

	conf := memberlist.DefaultLocalConfig()
	conf.Logger = log.New(io.Discard, "", 0)
	conf.BindPort = 8081
	conf.AdvertisePort = 8081
	conf.Delegate = &cluster
	conf.Events = &cluster
	conf.Name = managerConfig.Hostname

	clusterNodes, err := memberlist.Create(conf)
	if err != nil {
		return nil, err
	}

	cluster.cluster = clusterNodes

	return &cluster, nil
}

func (d *GossipCluster) NotifyMsg(msg []byte) {
	d.msgCh <- msg
}

func (d *GossipCluster) NodeMeta(limit int) []byte {
	logrus.Warn("NodeMeta")
	return nil
}

func (d *GossipCluster) LocalState(join bool) []byte {
	logrus.Warn("LocalState")
	return []byte("")
}

func (d *GossipCluster) GetBroadcasts(overhead, limit int) [][]byte {
	logrus.Warn("GetBroadcasts")
	return nil
}

func (d *GossipCluster) MergeRemoteState(buf []byte, join bool) {
	logrus.Warn("MergeRemoteState")
}

func (events *GossipCluster) NotifyJoin(node *memberlist.Node) {
	logrus.Infof("join %s", node.Name)
}

func (events *GossipCluster) NotifyLeave(node *memberlist.Node) {
	logrus.Infof("leave %s", node.Name)
}

func (events *GossipCluster) NotifyUpdate(node *memberlist.Node) {
	logrus.Infof("update %s", node.Name)
}
