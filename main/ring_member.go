package main

import "github.com/andrew-delph/my-key-store/rpc"

type RingMember struct {
	Name      string
	rpcClient rpc.RpcClient
}

func CreateRingMember(name string, rpcClient rpc.RpcClient) RingMember {
	return RingMember{Name: name, rpcClient: rpcClient}
}

func (m RingMember) String() string {
	return string(m.Name)
}
