package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

func baseHandler(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("handling / for %s", hostname)
	fmt.Fprint(w, "server is running")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("handling /health for %s validFSM = %v", hostname, validFSM)
	// if validFSM && raftNode.Leader() != "" {
	// if raftNode.Leader() != "" {
	if (raftNode.State() == raft.Follower || raftNode.State() == raft.Leader) && raftNode.Leader() != "" && validFSM {
		fmt.Fprintf(w, "validFSM %s", raftNode.State())
	} else {
		http.Error(w, "not validFSM", http.StatusBadRequest)
		// RaftTryLead()
		// AddAllMembers()
	}
}

func setHandler(w http.ResponseWriter, r *http.Request) {
	// Get the key and value from the query parameters
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	err := theManager.SendSetMessage(key, value)
	if err != nil {
		errorMessage := fmt.Sprintf("Set Error: key = '%s', value = '%s'. Error = %v", key, value, err)
		logrus.Error(errorMessage)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Fprintf(w, "Value has been set for key: %s value: %s", key, value)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	// Get the key from the query parameters
	key := r.URL.Query().Get("key")

	value, err := theManager.SendGetMessage(key)
	if err != nil {
		errorMessage := fmt.Sprintf("Get Error: key = '%s' Error = %v", key, err)
		logrus.Error(errorMessage)
		http.Error(w, errorMessage, http.StatusBadRequest)
		return
	}

	fmt.Fprintf(w, "Value for key '%s' is: '%s'", key, value)
}

func panicHandler(w http.ResponseWriter, r *http.Request) {
	curr := raftNode.State()
	go func() {
		time.Sleep(1 * time.Second)
		logrus.Fatalf("%s| EXCUTING PANIC!!!!!!!!!!!!!!!!!!!!!!", curr)
	}()
	fmt.Fprintf(w, "Recieved panic.")
}

func leaderHandler(w http.ResponseWriter, r *http.Request) {
	curr := raftNode.State()

	if curr == raft.Leader {
		go func() {
			time.Sleep(1 * time.Second)
			logrus.Fatalf("%s| EXCUTING PANIC!!!!!!! leaderHandler", curr)
		}()
		fmt.Fprintf(w, "Recieved panic.")
	} else {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "Server doesn't support hijacking", http.StatusInternalServerError)
			return
		}

		// Hijack the connection
		conn, _, err := hijacker.Hijack()
		if err != nil {
			http.Error(w, fmt.Sprintf("Connection hijacking failed: %v", err), http.StatusInternalServerError)
			return
		}

		// Close the connection
		conn.Close()
	}
}

func followerHandler(w http.ResponseWriter, r *http.Request) {
	curr := raftNode.State()

	if curr == raft.Follower {
		go func() {
			time.Sleep(1 * time.Second)
			logrus.Fatalf("%s| EXCUTING PANIC!!!!!!! leaderHandler", curr)
		}()
		fmt.Fprintf(w, "Recieved panic.")
	} else {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "Server doesn't support hijacking", http.StatusInternalServerError)
			return
		}

		// Hijack the connection
		conn, _, err := hijacker.Hijack()
		if err != nil {
			http.Error(w, fmt.Sprintf("Connection hijacking failed: %v", err), http.StatusInternalServerError)
			return
		}

		// Close the connection
		conn.Close()
	}
}

func bootstrapHandler(w http.ResponseWriter, r *http.Request) {
	err := RaftBootstrap()

	if err != nil {
		logrus.Warnf("RaftBootstrap failed: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "RaftBootstrap failed: %v", err)
	} else {
		logrus.Warnf("RaftBootstrap Success.")
		fmt.Fprintf(w, "RaftBootstrap Success.")
	}
}

// func removeHandler(w http.ResponseWriter, r *http.Request) {
// 	curr := raftNode.State()

// 	if curr == raft.Leader {
// 		// 172.26.0.2
// 		members := events.consistent.GetMembers()

// 		logrus.Warnf("TO_REMOVE = %s MINE = %s", members[0].String(), conf.Name)
// 		if members[0].String() == conf.Name {
// 			logrus.Warnf("USING NEXT!!! TO_REMOVE = %s MINE = %s", members[1].String(), conf.Name)
// 			RemoveServer(members[1].String())
// 		} else {
// 			RemoveServer(members[0].String())
// 		}

// 		fmt.Fprintf(w, "Recieved panic.")
// 	} else {
// 		hijacker, ok := w.(http.Hijacker)
// 		if !ok {
// 			http.Error(w, "Server doesn't support hijacking", http.StatusInternalServerError)
// 			return
// 		}

// 		// Hijack the connection
// 		conn, _, err := hijacker.Hijack()
// 		if err != nil {
// 			http.Error(w, fmt.Sprintf("Connection hijacking failed: %v", err), http.StatusInternalServerError)
// 			return
// 		}

// 		// Close the connection
// 		conn.Close()
// 	}
// }

func epochHandler(w http.ResponseWriter, r *http.Request) {
	curr := raftNode.State()

	if curr == raft.Leader {
		// 172.26.0.2
		logrus.Warnf("epochHandler start")
		err := UpdateEpoch()
		if err != nil {
			logrus.Warnf("epochHandler err =  %v", err)
			http.NotFound(w, r)
		} else {
			logrus.Warnf("epochHandler done")
			fmt.Fprintf(w, "Recieved epoch.")
		}

	} else {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "Server doesn't support hijacking", http.StatusInternalServerError)
			return
		}

		// Hijack the connection
		conn, _, err := hijacker.Hijack()
		if err != nil {
			http.Error(w, fmt.Sprintf("Connection hijacking failed: %v", err), http.StatusInternalServerError)
			return
		}

		// Close the connection
		conn.Close()
	}
}

func startHttpServer() {
	http.HandleFunc("/", baseHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/set", setHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/panic", panicHandler)
	http.HandleFunc("/leader", leaderHandler)
	http.HandleFunc("/follower", followerHandler)
	http.HandleFunc("/bootstrap", bootstrapHandler)
	// http.HandleFunc("/remove", removeHandler)
	http.HandleFunc("/epoch", epochHandler)
	http.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr: ":8080",
	}

	go func() {
		logrus.Infof("Server is running on http://localhost:8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.Panic(err)
		}
	}()

	// Set up a channel to listen for OS signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Block until we receive a SIGTERM or SIGINT
	<-stop

	// Create a context with a 5-second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Attempt a graceful shutdown
	if err := srv.Shutdown(ctx); err != nil {
		logrus.Fatalf("Server Shutdown Failed:%+v", err)
	}
	logrus.Warnf("Server gracefully stopped")
}
