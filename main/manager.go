package main

import (
	"os"
	"os/signal"
	"reflect"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/consensus"
	"github.com/andrew-delph/my-key-store/gossip"
	"github.com/andrew-delph/my-key-store/http"
	"github.com/andrew-delph/my-key-store/storage"
)

var numWorkers = 10

func mainTest() {
	logrus.Warn("hi")
}

type Manager struct {
	reqCh            chan interface{}
	db               storage.Storage
	httpServer       http.HttpServer
	gossipCluster    gossip.GossipCluster
	consensusCluster consensus.ConsensusCluster
}

func NewManager() Manager {
	// c := config.GetConfig()
	c := config.GetDefaultConfig()
	reqCh := make(chan interface{})

	httpServer := http.CreateHttpServer(reqCh)
	gossipCluster := gossip.CreateGossipCluster(c.Gossip, reqCh)
	db := storage.NewBadgerStorage(c.Storage)

	consensusCluster := consensus.CreateConsensusCluster(c.Consensus, reqCh)

	return Manager{reqCh: reqCh, db: db, httpServer: httpServer, gossipCluster: gossipCluster, consensusCluster: consensusCluster}
}

func (m Manager) StartManager() {
	var err error
	m.startWorkers()
	err = m.gossipCluster.Join()
	if err != nil {
		logrus.Fatal(err)
	}
	err = m.consensusCluster.StartConsensusCluster()
	if err != nil {
		logrus.Fatal(err)
	}
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
				err := m.db.Put([]byte(task.Key), []byte(task.Value))
				if err != nil {
					task.ResCh <- err
				} else {
					task.ResCh <- true
				}

			case http.GetTask:
				logrus.Debugf("worker GetTask: %+v", task)
				value, err := m.db.Get([]byte(task.Key))
				if err != nil {
					task.ResCh <- err
				} else {
					task.ResCh <- value
				}
			case gossip.JoinTask:
				logrus.Warnf("worker JoinTask: %+v", task)
			case gossip.LeaveTask:
				logrus.Warnf("worker LeaveTask: %+v", task)
			case consensus.LeaderChangeTask:
				logrus.Warnf("worker LeaderChangeTask: %+v", task)
			default:
				logrus.Fatalf("worker unkown task type: %v", reflect.TypeOf(task))
			}
		}
	}
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			logrus.Warn("Recovered from panic:", r)
		}
	}()
	logrus.Info("starting")
	manager := NewManager()
	manager.StartManager()
}
