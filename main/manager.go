package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/gossip"
	"github.com/andrew-delph/my-key-store/http"
	"github.com/andrew-delph/my-key-store/storage"
)

var numWorkers = 10

func mainTest() {
	logrus.Warn("hi")
}

type Manager struct {
	reqCh         chan interface{}
	db            storage.Storage
	httpServer    http.HttpServer
	gossipCluster gossip.GossipCluster
}

func NewManager() Manager {
	c := config.GetConfig()
	db := storage.NewBadgerStorage(c.Storage)
	gossipCluster := gossip.CreateGossipCluster(c.Gossip)
	reqCh := make(chan interface{})
	httpServer := http.NewHttpServer(reqCh)
	return Manager{reqCh: reqCh, db: db, httpServer: httpServer, gossipCluster: gossipCluster}
}

func (m Manager) StartManager() {
	err := m.gossipCluster.Join()
	if err != nil {
		logrus.Fatal(err)
	}
	go m.httpServer.StartHttp()
	for i := 0; i < numWorkers; i++ {
		go m.startWorker()
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	<-signals
	logrus.Warn("Received SIGTERM signal")
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
				logrus.Debugf("worker task: %+v", task)
				err := m.db.Put([]byte(task.Key), []byte(task.Value))
				if err != nil {
					task.ResCh <- err
				} else {
					task.ResCh <- true
				}

			case http.GetTask:
				logrus.Debugf("worker task: %+v", task)
				value, err := m.db.Get([]byte(task.Key))
				if err != nil {
					task.ResCh <- err
				} else {
					task.ResCh <- value
				}
			default:
				logrus.Fatal("worker unkown task type")
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
