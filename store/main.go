package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-zookeeper/zk"
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

// func main() {
// 	http.HandleFunc("/add", addHandler)
// 	http.HandleFunc("/get", getHandler)
// 	http.HandleFunc("/list", listHandler)

// 	fmt.Println("Server is running on port 8080...")
// 	if err := http.ListenAndServe(":8080", nil); err != nil {
// 		panic(err)
// 	}
// }

func watchGroup(conn *zk.Conn, path string) {
	for {
		children, _, watchChannel, err := conn.ChildrenW(path)
		if err != nil {
			log.Fatalf("Failed to list children for group %s: %v", path, err)
			return
		}
		log.Printf("Group members: %v", children)

		// Wait for changes in the group
		<-watchChannel
		log.Println("Group membership has changed!")
	}
}

func main() {
	servers := []string{"localhost:2181"}
	conn, _, err := zk.Connect(servers, time.Second)
	if err != nil {
		log.Fatalf("Unable to connect to ZooKeeper: %v", err)
	}
	defer conn.Close()

	groupPath := "/key-store"

	// Register the client to the group
	clientPath := groupPath + "/client-"
	_, err = conn.CreateProtectedEphemeralSequential(clientPath, []byte{}, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Fatalf("Unable to register client to the group: %v", err)
	}

	log.Println("Successfully registered to the key-store group!")

	// Watch for changes in the group
	go watchGroup(conn, groupPath)

	// Keep the connection open
	time.Sleep(10 * time.Minute)
}
