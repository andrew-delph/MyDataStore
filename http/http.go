package http

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/utils"
)

func testHttp() {
	logrus.Warn("HTTP")
}

type HttpServer struct {
	httpConfig config.HttpConfig
	reqCh      chan interface{}
	srv        *http.Server
}

type SetTask struct {
	Key   string
	Value string
	ResCh chan interface{}
}

type GetTask struct {
	Key   string
	ResCh chan interface{}
}

type HealthTask struct {
	ResCh chan interface{}
}

type ReadyTask struct {
	ResCh chan interface{}
}

func CreateHttpServer(httpConfig config.HttpConfig, reqCh chan interface{}) HttpServer {
	return HttpServer{httpConfig: httpConfig, reqCh: reqCh, srv: new(http.Server)}
}

func redirectRequest(w http.ResponseWriter, r *http.Request) {
	requestedURL := r.URL.String()
	logrus.Warn("requestedURL ", requestedURL)
	http.Redirect(w, r, requestedURL, http.StatusFound)
}

// Define a setHandler function
func (s HttpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	logrus.Debugf("http handler path = \"%s\" key = \"%s\" value: \"%s\" ", r.URL.Path, key, value)
	resCh := make(chan interface{})

	err := utils.WriteChannelTimeout(s.reqCh, SetTask{Key: key, Value: value, ResCh: resCh}, s.httpConfig.DefaultTimeout)
	if err != nil {
		redirectRequest(w, r)
		return
	}

	rawRes := utils.RecieveChannelTimeout(resCh, s.httpConfig.DefaultTimeout)
	switch res := rawRes.(type) {
	case string:
		fmt.Fprintf(w, res)
	case error:
		http.Error(w, fmt.Sprintf("%v hostname = %s", res, s.httpConfig.Hostname), http.StatusInternalServerError)
	default:
		logrus.Panicf("http unkown res type: %v", reflect.TypeOf(res))
	}
}

func (s HttpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	logrus.Debugf("http handler path = \"%s\" key = \"%s\"", r.URL.Path, key)
	resCh := make(chan interface{})

	err := utils.WriteChannelTimeout(s.reqCh, GetTask{Key: key, ResCh: resCh}, s.httpConfig.DefaultTimeout)
	if err != nil {
		redirectRequest(w, r)
		return
	}

	rawRes := utils.RecieveChannelTimeout(resCh, s.httpConfig.DefaultTimeout)
	switch res := rawRes.(type) {
	case string:
		fmt.Fprintf(w, res)
	case nil:
		http.Error(w, "value not found", http.StatusNotFound)
	case error:
		http.Error(w, fmt.Sprintf("%v hostname = %s", res, s.httpConfig.Hostname), http.StatusInternalServerError)
	default:
		logrus.Panicf("http unkown res type: %v", reflect.TypeOf(res))
	}
}

func (s HttpServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	resCh := make(chan interface{})

	err := utils.WriteChannelTimeout(s.reqCh, HealthTask{ResCh: resCh}, s.httpConfig.DefaultTimeout)
	if err != nil {
		logrus.Errorf("health err = %v", err)
		http.Error(w, "server busy", http.StatusBadRequest)
		return
	}

	rawRes := utils.RecieveChannelTimeout(resCh, 10)
	switch res := rawRes.(type) {
	case bool:
		if res {
			logrus.Debugf("health check bool = %v", res)
			fmt.Fprintf(w, "healthy")
		} else {
			logrus.Errorf("health check bool = %v", res)
			http.Error(w, "not healthy", http.StatusBadRequest)
		}
	case error:
		logrus.Errorf("health check err = %v", res)
		http.Error(w, res.Error(), http.StatusInternalServerError)
	default:
		logrus.Panicf("http unkown res type: %v", reflect.TypeOf(res))
	}
}

func (s HttpServer) readyHandler(w http.ResponseWriter, r *http.Request) {
	resCh := make(chan interface{})

	err := utils.WriteChannelTimeout(s.reqCh, ReadyTask{ResCh: resCh}, s.httpConfig.DefaultTimeout)
	if err != nil {
		logrus.Debugf("ready err = %v", err)
		http.Error(w, "server busy", http.StatusBadRequest)
		return
	}

	rawRes := utils.RecieveChannelTimeout(resCh, 10)
	switch res := rawRes.(type) {
	case bool:
		if res {
			logrus.Debugf("ready check bool = %v", res)
			fmt.Fprintf(w, "healthy")
		} else {
			logrus.Debugf("ready check bool = %v", res)
			http.Error(w, "not healthy", http.StatusBadRequest)
		}
	case error:
		logrus.Debugf("ready check err = %v", res)
		http.Error(w, res.Error(), http.StatusInternalServerError)
	default:
		logrus.Panicf("http unkown res type: %v", reflect.TypeOf(res))
	}
}

func (s HttpServer) StartHttp() {
	logrus.Debug("starting http server")
	http.HandleFunc("/set", s.setHandler)
	http.HandleFunc("/get", s.getHandler)
	http.HandleFunc("/health", s.healthHandler)
	http.HandleFunc("/ready", s.readyHandler)
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":8080", nil)
	srv := &http.Server{
		Addr: ":8080",
	}
	s.srv = srv

	go func() {
		logrus.Debugf("Server is running on http://localhost:8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.Panic(err)
		}
	}()
}

func (s HttpServer) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Attempt a graceful shutdown
	if err := s.srv.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}
