package main

import (
	"encoding/json"
	"fmt"
)

type MyMessage struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (m *MyMessage) Bytes() []byte {
	data, err := json.Marshal(m)
	if err != nil {
		return []byte("")
	}
	return data
}
func ParseMyMessage(data []byte) (*MyMessage, bool) {
	msg := new(MyMessage)
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, false
	}
	return msg, true
}

type Message interface {
	Encode() ([]byte, error)
	Decode([]byte) error
	Handle()
	GetType() string
}

type MessageHolder struct {
	MessageType  string
	MessageBytes []byte
}

type SetMessage struct {
	Key   string
	Value string
}

func (m *SetMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m SetMessage) GetType() string {
	return "SetMessage"
}

func (m *SetMessage) Decode(data []byte) error {
	return json.Unmarshal(data, m)
}

func (m *SetMessage) Handle() {
	fmt.Println("Handling SetMessage: ", m.Key, m.Value)
}

type AckMessage struct {
	success bool
}

func (m AckMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m *AckMessage) Handle() {
	fmt.Println("Handling AckMessage: ", m.success)
}

func (m AckMessage) GetType() string {
	return "AckMessage"
}

func (m *AckMessage) Decode(data []byte) error {
	return json.Unmarshal(data, m)
}

// Function to encode a MessageHolder
func EncodeHolder(msg Message) ([]byte, error) {
	messageBytes, err := msg.Encode()
	if err != nil {
		return nil, err
	}

	holder := MessageHolder{
		MessageType:  msg.GetType(),
		MessageBytes: messageBytes,
	}

	bytes, err := json.Marshal(holder)

	return bytes, err
}

func DecodeMessageHolder(data []byte) (Message, error) {
	holder := &MessageHolder{}
	err := json.Unmarshal(data, holder)
	if err != nil {
		fmt.Println("Failed to decode holder: ", err)
		return nil, err
	}

	switch holder.MessageType {
	case "SetMessage":
		msg := &SetMessage{}
		err := msg.Decode(holder.MessageBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to Decode SetMessage: %v", err)
		}
		return msg, nil
		// msg.Handle()
	case "AckMessage":
		msg := &AckMessage{}
		err := msg.Decode(holder.MessageBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to Decode AckMessage: %v", err)
		}
		return msg, nil
		// msg.Handle()
		// default:
		// 	return nil, errors.New("Unknown message type DEFAULT CASE")
	}
	return nil, fmt.Errorf("unknown message type: %s", holder.MessageType)
}
