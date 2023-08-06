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
	Handle(messageHolder *MessageHolder)
	GetType() string
}

type MessageHolder struct {
	MessageType  string
	SenderName   string
	MessageBytes []byte
}

type SetMessage struct {
	Key   string
	Value string
	AckId string
}

func NewSetMessage(key string, value string, ackID string) *SetMessage {
	return &SetMessage{
		Key:   key,
		Value: value,
		AckId: ackID,
	}
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

func (m *SetMessage) Handle(messageHolder *MessageHolder) {
	fmt.Printf("Handling SetMessage: key=%s value=%s ackId=%s sender=%s\n", m.Key, m.Value, m.AckId, messageHolder.SenderName)
	events.SendAckMessage(m.AckId, messageHolder.SenderName)
}

type AckMessage struct {
	AckId   string
	Success bool
}

func NewAckMessage(ackID string, success bool) *AckMessage {
	return &AckMessage{
		AckId:   ackID,
		Success: success,
	}
}

func (m AckMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m *AckMessage) Handle(messageHolder *MessageHolder) {
	fmt.Println("Handling AckMessage: ", m.Success, m.AckId)
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
		SenderName:   clusterNodes.LocalNode().Name,
	}

	bytes, err := json.Marshal(holder)

	return bytes, err
}

func DecodeMessageHolder(data []byte) (*MessageHolder, Message, error) {
	holder := &MessageHolder{}
	err := json.Unmarshal(data, holder)
	if err != nil {
		fmt.Println("Failed to decode holder: ", err)
		return nil, nil, err
	}

	switch holder.MessageType {
	case "SetMessage":
		msg := &SetMessage{}
		err := msg.Decode(holder.MessageBytes)
		if err != nil {
			return holder, nil, fmt.Errorf("failed to Decode SetMessage: %v", err)
		}
		return holder, msg, nil
	case "AckMessage":
		msg := &AckMessage{}
		err := msg.Decode(holder.MessageBytes)
		if err != nil {
			return holder, nil, fmt.Errorf("failed to Decode AckMessage: %v", err)
		}
		return holder, msg, nil
	}
	return holder, nil, fmt.Errorf("unknown message type: %s", holder.MessageType)
}
