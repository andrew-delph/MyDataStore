package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

var numWorkers = 10

func mainTest() {
	logrus.Warn("hi")
}

type Manager struct {
	reqCh chan interface{}
}

func NewManager() Manager {
	ch := make(chan interface{})
	return Manager{reqCh: ch}
}

func (m Manager) StartManager() {
	httpServer := NewHttpServer(m.reqCh)
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
			case SetTask:
				logrus.Infof("worker task: %+v", task)
				time.Sleep(1 * time.Millisecond)
				task.resCh <- task.Value
			case GetTask:
				logrus.Infof("worker task: %+v", task)
				time.Sleep(1 * time.Millisecond)
				task.resCh <- task.Key
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
