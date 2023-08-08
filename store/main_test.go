package main

import (
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/hashicorp/memberlist"
	"gotest.tools/assert"
)

// Define an interface to abstract the Memberlist
type Memberlister interface {
	LocalNode() Noder
}

// Define an interface to abstract the Node
type Noder interface {
	Name() string
}

// Then create structs that implement these interfaces

type MockMemberlist struct{}

func (m *MockMemberlist) LocalNode() Noder {
	return &MockNode{}
}

type MockNode struct{}

func (m *MockNode) Name() string {
	return "mock node name"
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
	var err error
	clusterNodes, err = memberlist.Create(memberlist.DefaultLocalConfig())
	if err != nil {
		t.Fatalf("Failed to memberlist.Create: %v", err)
	}

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
	decodedMessageHolder, decodedMessage, err := DecodeMessageHolder(encodedHolder)
	if err != nil {
		t.Fatal(err)
	}

	logrus.Println("decodedMessageHolder", decodedMessageHolder)
	logrus.Println("decodedMessage", decodedMessage)

	assert.Equal(t, decodedMessage.GetType(), "SetMessage", "Both should be SetMessage")

}
