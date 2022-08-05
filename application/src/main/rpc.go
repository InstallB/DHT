package main

import (
	"errors"
	"github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"time"
)

func Ping(addr string) (bool, error) {
	if addr == NULL {
		logrus.Trace("ping a NULL address")
		return false, errors.New("ping a NULL address")
	}
	ErrorChannel := make(chan error)
	go func() {
		client, err := rpc.Dial("tcp", addr)
		if err == nil {
			CloseClient(client)
			ErrorChannel <- nil
		} else {
			ErrorChannel <- err
		}
	}()
	select {
	case err := <-ErrorChannel:
		if err == nil {
			logrus.Trace("Ping success: ", addr)
			return true, nil
		} else {
			logrus.Trace("Ping failed: ", addr)
			return false, err
		}
	case <-time.After(PingWaitTime * 1):
		logrus.Trace("Ping Time Out: ", addr)
		return false, errors.New("time out")
	}
}

func Accept(listener net.Listener, server *rpc.Server, n *Mynode) {
	for {
		conn, err := listener.Accept()
		select {
		case <-n.QuitSignal:
			logrus.Info("Accept quit: ", n.Addr)
			return
		default:
			if err != nil {
				logrus.Error("listener.Accept failed in Accept(", listener, server, ") Error: ", err)
				return
			}
			go server.ServeConn(conn)
		}

	}
}

func CloseClient(client *rpc.Client) {
	err := client.Close()
	if err != nil {
		logrus.Error("close_client failed")
	}
}

func RpcCall(addr string, method string, args any, reply any) error {
	logrus.Info("RpcCall(", addr, " ", method, " ", args, " ", reply, ")")
	if v, err := Ping(addr); v == false {
		logrus.Error("Could not connect to ", addr, " in rpc_call(", addr, " ", method, " ", args, " ", reply, ") Error: ", err)
		return err
	}
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		logrus.Error("Dial failed in rpc_call(", addr, " ", method, " ", args, " ", reply, ") Error: ", err)
		return err
	}
	defer CloseClient(client)
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error("Call failed in rpc_call(", addr, " ", method, " ", args, " ", reply, ") Error: ", err)
		return err
	}
	return nil
}
