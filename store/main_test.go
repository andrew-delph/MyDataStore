package main

import (
	"log"
	"testing"

	"github.com/serialx/hashring"
	"gotest.tools/assert"
)

func TestHashRing(t *testing.T) {
	// Create an empty hash ring
	hashRing := hashring.New([]string{})

	// Add two nodes to the hash ring
	hashRing = hashRing.AddNode("node1")
	hashRing = hashRing.AddNode("node2")

	// Print the nodes in the hash ring
	log.Println(hashRing.GetNode("test"))
	log.Println("size", hashRing.Size())
}

func TestEncodeDecodeSetMessage(t *testing.T) {
	setMsg := &SetMessage{
		Key:   "testKey",
		Value: "testValue",
	}

	// Test encoding
	encodedMsg, err := setMsg.Encode()
	if err != nil {
		t.Fatalf("Failed to encode SetMessage: %v", err)
	}

	// Test decoding
	decodedMsg := &SetMessage{}
	err = decodedMsg.Decode(encodedMsg)
	if err != nil {
		t.Fatalf("Failed to decode SetMessage: %v", err)
	}

	// Check if the decoded message matches the original one
	if decodedMsg.Key != setMsg.Key || decodedMsg.Value != setMsg.Value {
		t.Fatalf("Decoded SetMessage does not match the original one: got %v want %v",
			decodedMsg, setMsg)
	}
}

func TestEncodeDecodeMessageHolderWithSetMessage(t *testing.T) {
	setMsg := &SetMessage{
		Key:   "testKey",
		Value: "testValue",
	}

	// Wrap SetMessage in MessageHolder
	holder := MessageHolder{
		MessageType:  setMsg.GetType(),
		MessageBytes: nil,
	}

	// Encode SetMessage
	encodedSetMsg, err := setMsg.Encode()
	if err != nil {
		t.Fatalf("Failed to encode SetMessage: %v", err)
	}

	// Put encoded SetMessage into MessageHolder
	holder.MessageBytes = encodedSetMsg

	// Encode MessageHolder
	encodedHolder, err := EncodeHolder(setMsg)
	if err != nil {
		t.Fatal(err)
	}

	// Decode MessageHolder
	decodedMessage, err := DecodeMessageHolder(encodedHolder)
	if err != nil {
		t.Fatal(err)
	}

	log.Println("decodedMessage", decodedMessage)

	assert.Equal(t, decodedMessage.GetType(), "SetMessage", "Both should be SetMessage")

}
