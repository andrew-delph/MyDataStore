package consensus

import (
	"github.com/sirupsen/logrus"
)

func consensusTest() {
	logrus.Warn("CONSENSUS")
}

type ConsensusCluster struct {
	reqCh chan interface{}
}

func CreateConsensusCluster() ConsensusCluster {
	return ConsensusCluster{}
}
