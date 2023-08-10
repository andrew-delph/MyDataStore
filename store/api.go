package main

import (
	"fmt"
	"net/http"

	"github.com/sirupsen/logrus"
)

func baseHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "server is running")
}

func setHandler(w http.ResponseWriter, r *http.Request) {
	// Get the key and value from the query parameters
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	// Store the value associated with the key
	// store[key] = value

	err := SendSetMessage(key, value)
	if err != nil {
		errorMessage := fmt.Sprintf("Could not set value: key = '%s', value = '%s'. Error = %v", key, value, err)
		http.Error(w, errorMessage, http.StatusBadRequest)
		return
	}

	fmt.Fprintf(w, "Value has been set for key: %s value: %s", key, value)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	// Get the key from the query parameters
	key := r.URL.Query().Get("key")

	value, err := SendGetMessage(key)
	if err != nil {
		errorMessage := fmt.Sprintf("Could not set value: key = '%s', value = '%s'. Error = %v", key, value, err)
		http.Error(w, errorMessage, http.StatusBadRequest)
		return
	}

	fmt.Fprintf(w, "Value for key '%s' is: '%s'", key, value)
}

func panicHandler(w http.ResponseWriter, r *http.Request) {
	logrus.Panic("Recieved panic request")
}

func startHttpServer() {
	http.HandleFunc("/", baseHandler)
	http.HandleFunc("/set", setHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/panic", panicHandler)

	logrus.Info("Server is running on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		logrus.Panic(err)
	}
}
