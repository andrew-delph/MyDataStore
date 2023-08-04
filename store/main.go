package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Global map
var myMap = map[string]string{}

func addValue(key, value string) {
	myMap[key] = value
}

// HTTP handler for adding a value
func addHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	addValue(key, value)
	fmt.Fprintf(w, "Added %s: %s to the map", key, value)
}

func getValue(key string) string {
	return myMap[key]
}

// HTTP handler for getting a value
func getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := getValue(key)
	fmt.Fprintf(w, "Value for %s: %s", key, value)
}

func listValues() map[string]string {
	return myMap
}

// HTTP handler for listing all values
func listHandler(w http.ResponseWriter, r *http.Request) {
	values := listValues()
	json.NewEncoder(w).Encode(values)
}

func main() {
	http.HandleFunc("/add", addHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/list", listHandler)

	fmt.Println("Server is running on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
