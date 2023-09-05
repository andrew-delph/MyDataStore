package main

import (
	"fmt"
	"net/http"

	"github.com/sirupsen/logrus"
)

type HttpServer struct {
	reqCh chan interface{}
}

type SetTask struct {
	Key   string
	Value string
	resCh chan interface{}
}

type GetTask struct {
	Key   string
	resCh chan interface{}
}

// Define a setHandler function
func (s HttpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	logrus.Debugf("http handler path = \"%s\" key = \"%s\" value: \"%s\" ", r.URL.Path, key, value)
	resCh := make(chan interface{})
	s.reqCh <- SetTask{Key: key, Value: value, resCh: resCh}
	res := <-resCh
	fmt.Fprintf(w, "%s", res)
}

func (s HttpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	logrus.Debugf("http handler path = \"%s\" key = \"%s\"", r.URL.Path, key)
	resCh := make(chan interface{})
	s.reqCh <- GetTask{Key: key, resCh: resCh}
	res := <-resCh
	fmt.Fprintf(w, "%s", res)
}

func (s HttpServer) StartHttp() {
	logrus.Info("starting http server")
	// Register the handler function for the root route
	http.HandleFunc("/set", s.setHandler)
	http.HandleFunc("/get", s.getHandler)
	// Start the HTTP server on port 8080
	http.ListenAndServe(":8080", nil)
}

func NewHttpServer(ch chan interface{}) HttpServer {
	return HttpServer{reqCh: ch}
}
