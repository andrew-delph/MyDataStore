package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/http"
	"github.com/andrew-delph/my-key-store/storage"
)

var numWorkers = 10

func mainTest() {
	logrus.Warn("hi")
}

type Manager struct {
	reqCh chan interface{}
	db    storage.Storage
}

func NewManager() Manager {
	c := config.GetConfig()
	db := storage.NewBadgerStorage(c.Storage)
	// db := storage.NewLevelDbStorage(c.Storage)
	ch := make(chan interface{})
	return Manager{reqCh: ch, db: db}
}

func (m Manager) StartManager() {
	httpServer := http.NewHttpServer(m.reqCh)
	go httpServer.StartHttp()

	for i := 0; i < numWorkers; i++ {
		go m.startWorker()
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	<-signals
	logrus.Warn("Received SIGTERM signal")
}

func (m Manager) startWorker() {
	logrus.Info("starting worker")
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
