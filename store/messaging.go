package main

import (
	"encoding/json"
	"fmt"
	"log"
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
	Message      Message
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
	log.Printf("Handling SetMessage: key=%s value=%s ackId=%s sender=%s\n", m.Key, m.Value, m.AckId, messageHolder.SenderName)
	setValue(m.Key, m.Value)
	events.SendAckMessage("", m.AckId, messageHolder.SenderName, true)
}

type GetMessage struct {
	Key   string
	AckId string
}

func NewGetMessage(key string, ackID string) *GetMessage {
	return &GetMessage{
		Key:   key,
		AckId: ackID,
	}
}

func (m *GetMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m GetMessage) GetType() string {
	return "GetMessage"
}

func (m *GetMessage) Decode(data []byte) error {
	return json.Unmarshal(data, m)
}

func (m *GetMessage) Handle(messageHolder *MessageHolder) {
	log.Printf("Handling GetMessage: key=%s ackId=%s sender=%s\n", m.Key, m.AckId, messageHolder.SenderName)
	value, exists := getValue(m.Key)
	events.SendAckMessage(value, m.AckId, messageHolder.SenderName, exists)
}

type AckMessage struct {
	AckId   string
	Success bool
	Value   string
}

func NewAckMessage(ackID string, success bool, value string) *AckMessage {
	return &AckMessage{
		AckId:   ackID,
		Success: success,
		Value:   value,
	}
}

func (m AckMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m *AckMessage) Handle(messageHolder *MessageHolder) {
	log.Printf("Handling AckMessage. sender=%s success=%t ackId=%s value=%s", messageHolder.SenderName, m.Success, m.AckId, m.Value)
	ackChannel, exists := getAckChannel(m.AckId)

	if !exists {
		log.Println("ackChannel does not exist")
		return
	}

	ackChannel <- messageHolder
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
	case "GetMessage":
		msg := &GetMessage{}
		err := msg.Decode(holder.MessageBytes)
		if err != nil {
			return holder, nil, fmt.Errorf("failed to Decode GetMessage: %v", err)
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
