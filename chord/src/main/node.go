package main

import (
	"github.com/sirupsen/logrus"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */

func NewNode(port int) DhtNode {
	var i Mynode
	i.Initialize(GetLocalAddress() + ":" + strconv.Itoa(port))
	var a DhtNode = &i
	return a
}

type Mynode struct {
	Addr string

	Predecessor   string
	PreLock       sync.RWMutex
	Successor     string
	SuccessorList [SuccessorListLength]string
	SucLock       sync.RWMutex
	Finger        [M]string
	FingerLock    sync.RWMutex

	Data       map[string]string
	DataLock   sync.RWMutex
	BackupData map[string]string
	BackupLock sync.RWMutex

	Online   bool
	Server   *rpc.Server
	Listener net.Listener

	QuitSignal chan bool
}

func (n *Mynode) Initialize(addr string) {
	logrus.Info("Initialize: ", addr)
	n.Online = false
	n.Addr = addr
	n.Data = make(map[string]string)
	n.BackupData = make(map[string]string)
	n.Server = rpc.NewServer()
	n.QuitSignal = make(chan bool, 2)
}

func (n *Mynode) Reset() {
	logrus.Info("Node Reset: ", n.Addr)
	n.Addr = NULL
	n.Predecessor = NULL
	n.Successor = NULL
	n.Data = make(map[string]string)
	n.BackupData = make(map[string]string)
	for i := 0; i < M; i++ {
		n.Finger[i] = NULL
	}
	for i := 0; i < SuccessorListLength; i++ {
		n.SuccessorList[i] = NULL
	}
	n.QuitSignal = make(chan bool, 2)
}

func (n *Mynode) InitializeServer() {
	logrus.Info("Initialize Server: ", n.Addr)
	n.Server = rpc.NewServer()
	err := n.Server.Register(n)
	if err != nil {
		logrus.Error("Register failed in Initialize_server() of ", n.Addr, " Error: ", err)
		return
	}
	n.Listener, err = net.Listen("tcp", n.Addr)
	if err != nil {
		logrus.Error("Listen failed in Initialize_server() of ", n.Addr, " Error: ", err)
		return
	}
	go Accept(n.Listener, n.Server, n)
}

func (n *Mynode) GetPredecessor(_ string, ret *string) error {
	n.PreLock.RLock()
	*ret = n.Predecessor
	n.PreLock.RUnlock()
	return nil
}

func (n *Mynode) SetPredecessor(p string, _ *string) error {
	n.PreLock.Lock()
	n.Predecessor = p
	n.PreLock.Unlock()
	return nil
}

func (n *Mynode) GetSuccessor(_ string, ret *string) error {
	n.SucLock.RLock()
	*ret = n.Successor
	n.SucLock.RUnlock()
	return nil
}

func (n *Mynode) SetSuccessor(p string, _ *string) error {
	n.SucLock.Lock()
	n.Successor = p
	n.SucLock.Unlock()
	return nil
}

func (n *Mynode) GetSuccessorList(_ string, ret *[SuccessorListLength]string) error {
	n.SucLock.RLock()
	for i := 0; i < SuccessorListLength; i++ {
		(*ret)[i] = n.SuccessorList[i]
	}
	n.SucLock.RUnlock()
	return nil
}

func (n *Mynode) CheckPredecessor() {
	var pre string
	_ = n.GetPredecessor(NULL, &pre)
	if pre != NULL && !n.Ping(pre) {
		n.StoreBackup()
		n.PreLock.Lock()
		n.Predecessor = NULL
		n.PreLock.Unlock()
	}
}

func (n *Mynode) StoreBackup() {
	var suc string
	err := n.FirstAvailableSuccessor(NULL, &suc)
	if err != nil {
		return
	}
	n.BackupLock.Lock()
	n.DataLock.Lock()
	for key, value := range n.BackupData {
		n.Data[key] = value
	}
	n.DataLock.Unlock()
	n.BackupLock.Unlock()
	if suc == n.Addr {
		return
	}
	n.BackupLock.RLock()
	_ = RpcCall(suc, "Mynode.ReceiveBackup", &n.BackupData, nil)
	n.BackupLock.RUnlock()
	n.BackupLock.Lock()
	n.BackupData = make(map[string]string)
	n.BackupLock.Unlock()
}

func (n *Mynode) FirstAvailableSuccessor(_ string, ret *string) error {
	for i := 0; i < SuccessorListLength; i++ {
		n.SucLock.RLock()
		if n.SuccessorList[i] == NULL {
			n.SucLock.RUnlock()
			break
		}
		if n.Ping(n.SuccessorList[i]) {
			*ret = n.SuccessorList[i]
			n.SucLock.RUnlock()
			return nil
		} else {
			n.SucLock.RUnlock()
			n.SucLock.Lock()
			for j := i; j < SuccessorListLength-1; j++ {
				n.SuccessorList[j] = n.SuccessorList[j+1]
			}
			n.SuccessorList[SuccessorListLength-1] = NULL
			if i == 0 {
				n.Successor = n.SuccessorList[0]
			}
			n.SucLock.Unlock()
		}
	}
	err := n.FindSuccessor(GetId(n.Addr), ret)
	if err != nil {
		return err
	}
	n.SucLock.Lock()
	n.SuccessorList[0] = *ret
	n.Successor = *ret
	n.SucLock.Unlock()
	return nil
}

func (n *Mynode) FindSuccessor(Id *big.Int, ret *string) error {
	return n.FindPredecessor(Id, ret)
} // return addr of node

func (n *Mynode) FindPredecessor(Id *big.Int, ret *string) error {
	n.SucLock.RLock()
	if InRange(Id, new(big.Int).Add(GetId(n.Addr), big.NewInt(1)), GetId(n.Successor)) {
		*ret = n.Successor
		n.SucLock.RUnlock()
		return nil
	}
	n.SucLock.RUnlock()
	nxt := n.ClosestPrecedingFinger(Id)
	err := RpcCall(nxt, "Mynode.FindPredecessor", Id, ret)
	return err
} // return addr of node

func (n *Mynode) ClosestPrecedingFinger(id *big.Int) string {
	nid := GetId(n.Addr)
	n.FingerLock.RLock()
	for i := M - 1; i >= 0; i-- {
		if InRange(GetId(n.Finger[i]), new(big.Int).Add(nid, big.NewInt(1)), new(big.Int).Sub(id, big.NewInt(1))) && n.Ping(n.Finger[i]) {
			ret := n.Finger[i]
			n.FingerLock.RUnlock()
			return ret
		}
	}
	n.FingerLock.RUnlock()
	return n.Successor
} // return addr of node

func (n *Mynode) Run() {
	n.InitializeServer()
	n.Maintain()
}

func (n *Mynode) Create() {
	logrus.Info(n.Addr, " Create new network")
	n.Online = true
	n.PreLock.Lock()
	n.Predecessor = n.Addr
	n.PreLock.Unlock()
	n.SucLock.Lock()
	n.Successor = n.Addr
	n.SuccessorList[0] = n.Addr
	for i := 1; i < SuccessorListLength; i++ {
		n.SuccessorList[i] = NULL
	}
	n.SucLock.Unlock()
	n.FingerLock.Lock()
	for i := 0; i < M; i++ {
		n.Finger[i] = n.Addr
	}
	n.FingerLock.Unlock()
}

func (n *Mynode) InitFingerTable(s *string) {
	var tar string
	_ = n.FirstAvailableSuccessor(NULL, &tar)
	n.FingerLock.Lock()
	n.Finger[0] = tar
	n.FingerLock.Unlock()
	nid := GetId(n.Addr)
	for i := 0; i < M-1; i++ {
		start := new(big.Int).Add(nid, pow2((int64)(i+1)))
		if InRange(start, nid, new(big.Int).Sub(GetId(n.Finger[i]), big.NewInt(1))) {
			n.Finger[i+1] = n.Finger[i]
		} else {
			err := RpcCall(*s, "Mynode.FindSuccessor", start, &n.Finger[i+1])
			if err != nil {
				return
			}
		}
	}
}

func (n *Mynode) UpdateOthers() {
	nid := GetId(n.Addr)
	for i := 0; i < M; i++ {
		var pre string
		err := n.FindPredecessor(new(big.Int).Sub(nid, pow2((int64)(i-1))), &pre)
		if err != nil {
			return
		}
		err = RpcCall(pre, "Mynode.UpdateFingerTable", Pair2{n.Addr, i}, nil)
		if err != nil {
			return
		}
	}
}

func (n *Mynode) UpdateFingerTable(pi Pair2, _ *string) error {
	s := pi.First
	i := pi.Second
	if InRange(GetId(s), GetId(n.Addr), new(big.Int).Sub(GetId(n.Finger[i]), big.NewInt(1))) {
		n.PreLock.RLock()
		pre := n.Predecessor
		n.PreLock.RUnlock()
		n.FingerLock.Lock()
		n.Finger[i] = s
		n.FingerLock.Unlock()
		err := RpcCall(pre, "Mynode.UpdateFingerTable", Pair2{s, i}, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *Mynode) TransferData(id *big.Int, m *map[string]string) error {
	del := make(map[string]string)
	n.DataLock.RLock()
	n.BackupLock.Lock()
	for key, value := range n.Data {
		if !InRange(GetId(key), new(big.Int).Add(id, big.NewInt(1)), GetId(n.Addr)) {
			(*m)[key] = value
			del[key] = value
			n.BackupData[key] = value
		}
	}
	n.BackupLock.Unlock()
	n.DataLock.RUnlock()
	n.DataLock.Lock()
	for key := range del {
		delete(n.Data, key)
	}
	n.DataLock.Unlock()
	var nxt string
	_ = n.FirstAvailableSuccessor(NULL, &nxt)
	_ = RpcCall(nxt, "Mynode.RemoveBackup", &del, nil)
	return nil
}

func (n *Mynode) TransferBackupData(_ string, m *map[string]string) error {
	n.BackupLock.RLock()
	for key, value := range n.Data {
		(*m)[key] = value
	}
	n.BackupLock.RUnlock()
	return nil
}

func (n *Mynode) RemoveBackup(m *map[string]string, _ *string) error {
	n.BackupLock.Lock()
	for key := range *m {
		delete(n.BackupData, key)
	}
	n.BackupLock.Unlock()
	return nil
}

func (n *Mynode) ReceiveBackup(m *map[string]string, _ *string) error {
	n.BackupLock.Lock()
	for key, value := range *m {
		n.BackupData[key] = value
	}
	n.BackupLock.Unlock()
	return nil
}

func (n *Mynode) Join(addr string) bool {
	// addr != NULL
	logrus.Info(n.Addr, " Node Join Through ", addr)
	n.Predecessor = NULL

	var tar string
	err := RpcCall(addr, "Mynode.FindSuccessor", GetId(n.Addr), &tar)
	if err != nil {
		return false
	}
	n.SucLock.Lock()
	n.Successor = tar
	n.SuccessorList[0] = tar
	suc := tar
	n.SucLock.Unlock()

	_ = RpcCall(suc, "Mynode.GetPredecessor", NULL, &tar)
	n.PreLock.Lock()
	n.Predecessor = tar
	n.PreLock.Unlock()
	_ = RpcCall(suc, "Mynode.SetPredecessor", n.Addr, nil)
	_ = RpcCall(tar, "Mynode.SetSuccessor", n.Addr, nil) // tar is predecessor

	n.DataLock.Lock()
	err = RpcCall(suc, "Mynode.TransferData", GetId(n.Addr), &n.Data)
	n.DataLock.Unlock()
	if err != nil {
		return false
	}

	n.BackupLock.Lock()
	err = RpcCall(tar, "Mynode.TransferBackupData", NULL, &n.BackupData)
	n.BackupLock.Unlock()
	if err != nil {
		return false
	}

	n.InitFingerTable(&addr)

	n.Online = true
	return true
}

func (n *Mynode) Ping(addr string) bool {
	ret, err := Ping(addr)
	if err != nil {
		return false
	}
	return ret
}

func (n *Mynode) PutData(pi Pair, _ *string) error {
	logrus.Info(n.Addr, " PutData: [", pi.First, ",", pi.Second, "]")
	n.DataLock.Lock()
	n.Data[pi.First] = pi.Second
	n.DataLock.Unlock()
	var tar string
	err := n.FirstAvailableSuccessor(NULL, &tar)
	if err != nil {
		return err
	}
	err = RpcCall(tar, "Mynode.PutBackup", pi, nil)
	return err
}

func (n *Mynode) PutBackup(pi Pair, _ *string) error {
	logrus.Info(n.Addr, " PutBackup: [", pi.First, ",", pi.Second, "]")
	n.BackupLock.Lock()
	n.BackupData[pi.First] = pi.Second
	n.BackupLock.Unlock()
	return nil
}

func (n *Mynode) Put(key string, value string) bool {
	var tar string
	err := n.FindSuccessor(GetId(key), &tar)
	if err != nil {
		logrus.Info(n.Addr, " FindSuccessor in Put Failed, Error: ", err)
		return false
	}
	err = RpcCall(tar, "Mynode.PutData", Pair{key, value}, nil)
	if err != nil {
		logrus.Error(n.Addr, " Put Failed, Error: ", err)
		return false
	}
	logrus.Info(n.Addr, " Successfully Put [", key, ',', value, ']')
	return true
}

func (n *Mynode) GetData(key string, value *string) error {
	n.DataLock.RLock()
	ret, ok := n.Data[key]
	n.DataLock.RUnlock()
	if ok == true {
		*value = ret
	} else {
		*value = NULL
	}
	logrus.Info(n.Addr, " GetData: [", key, ",", *value, "]")
	return nil
}

func (n *Mynode) Get(key string) (bool, string) {
	logrus.Info(n.Addr, " Get: [", key, "]")
	var tar string
	var ret string
	for i := 1; i <= 5; i++ {
		err := n.FindSuccessor(GetId(key), &tar)
		if err != nil {
			logrus.Error("[", n.Addr, "] Get try ", i, " : Error ", err)
			time.Sleep(MapWaitTime)
			continue
		}
		err = RpcCall(tar, "Mynode.GetData", key, &ret)
		if err == nil && ret != NULL {
			logrus.Info("[", n.Addr, "] Get try ", i, " : Success Get [", key, ',', ret, ']')
			return true, ret
		} else {
			if err == nil {
				logrus.Info("[", n.Addr, "] Get try ", i, " : Failed Key not found")
			} else {
				logrus.Error("[", n.Addr, "] Get try ", i, " : Failed Error: ", err)
			}
		}
		time.Sleep(MapWaitTime)
	}
	return false, NULL
} // try multiple times, with some interval

func (n *Mynode) DeleteData(key string, ret *bool) error {
	logrus.Info(n.Addr, " DeleteData: [", key, "]")
	n.DataLock.Lock()
	_, ok := n.Data[key]
	if ok == true {
		delete(n.Data, key)
		n.DataLock.Unlock()
		*ret = true
	} else {
		n.DataLock.Unlock()
		*ret = false
	}
	var tar string
	err := n.FirstAvailableSuccessor(NULL, &tar)
	if err != nil {
		return err
	}
	err = RpcCall(tar, "Mynode.DeleteBackup", key, nil)
	return err
}

func (n *Mynode) DeleteBackup(key string, _ *bool) error {
	logrus.Info(n.Addr, " DeleteBackup: [", key, "]")
	n.BackupLock.Lock()
	_, ok := n.BackupData[key]
	if ok == true {
		delete(n.Data, key)
	}
	n.BackupLock.Unlock()
	return nil
}

func (n *Mynode) Delete(key string) bool {
	logrus.Info(n.Addr, " Delete: [", key, "]")
	var tar string
	var ret bool
	for i := 1; i <= 5; i++ {
		err := n.FindSuccessor(GetId(key), &tar)
		if err != nil {
			logrus.Error(n.Addr, " Error in FindSuccesssor in Delete(", key, "), Error: ", err)
			time.Sleep(MapWaitTime)
			continue
		}
		err = RpcCall(tar, "Mynode.DeleteData", key, &ret)
		if err == nil && ret == true {
			return true
		}
		if err != nil {
			logrus.Error(n.Addr, " Error DeleteData in Delete(", key, "), Error: ", err)
		}
		time.Sleep(MapWaitTime)
	}
	return false
} // try multiple times, with some interval

func (n *Mynode) Stabilize() {
	if !n.Online {
		return
	}
	var tar, nxt string
	tar = NULL
	nxt = NULL
	_ = n.FirstAvailableSuccessor(NULL, &nxt)
	err := RpcCall(nxt, "Mynode.GetPredecessor", NULL, &tar)
	if err != nil {
		return
	}

	if tar != NULL && tar != n.Addr && InRange(GetId(tar), new(big.Int).Add(GetId(n.Addr), big.NewInt(1)), new(big.Int).Sub(GetId(nxt), big.NewInt(1))) {
		n.SucLock.Lock()
		n.Successor = tar
		n.SuccessorList[0] = tar
		nxt = tar
		n.SucLock.Unlock()
	}

	var list [SuccessorListLength]string
	err = RpcCall(nxt, "Mynode.GetSuccessorList", NULL, &list)
	if err != nil {
		logrus.Error(n.Addr, ",", nxt, " GetSuccessorList Failed in Stabilize, Error: ", err)
		return
	}
	pos := 1
	for i := 0; i < SuccessorListLength; i++ {
		if n.Ping(list[i]) {
			n.SucLock.Lock()
			n.SuccessorList[pos] = list[i]
			n.SucLock.Unlock()
			pos++
			if pos == SuccessorListLength {
				break
			}
		}
	}
	err = RpcCall(nxt, "Mynode.Notify", n.Addr, nil)
	if err != nil {
		logrus.Error(n.Addr, ",", nxt, " Notify Failed in Stabilize")
		return
	}
}

func (n *Mynode) DataToBackup(_ string, m *map[string]string) error {
	n.DataLock.RLock()
	for key, value := range n.Data {
		(*m)[key] = value
	}
	n.DataLock.RUnlock()
	return nil
}

func (n *Mynode) Notify(addr string, _ *bool) error {
	logrus.Info(addr, " Notify ", n.Addr)
	nid := GetId(addr)
	pre := n.Predecessor
	if pre == NULL || InRange(nid, new(big.Int).Add(GetId(pre), big.NewInt(1)), new(big.Int).Sub(GetId(n.Addr), big.NewInt(1))) {
		n.StoreBackup()
		n.PreLock.Lock()
		n.Predecessor = addr
		n.PreLock.Unlock()
		n.BackupLock.Lock()
		_ = RpcCall(addr, "Mynode.DataToBackup", NULL, &n.BackupData)
		n.BackupLock.Unlock()
	}
	return nil
}

func (n *Mynode) FixFinger() {
	for j := 1; j <= 10; j++ {
		i := rand.Int() % M
		err := n.FindSuccessor(new(big.Int).Add(GetId(n.Addr), pow2((int64)(i))), &n.Finger[i])
		if err != nil {
			return
		}
	}
}

func (n *Mynode) Maintain() {
	go func() {
		for {
			if n.Online {
				n.Stabilize()
			}
			time.Sleep(MaintainWaitTime)
		}
	}()
	go func() {
		for {
			if n.Online {
				n.FixFinger()
			}
			time.Sleep(MaintainWaitTime)
		}
	}()
	go func() {
		for {
			if n.Online {
				n.CheckPredecessor()
			}
			time.Sleep(MaintainWaitTime)
		}
	}()
}

func (n *Mynode) Close() {
	n.Online = false
	n.QuitSignal <- true
	err := n.Listener.Close()
	if err != nil {
		logrus.Error(n.Addr, " Listener Close Failed")
		return
	}
}

func (n *Mynode) ReceiveData(m *map[string]string, _ *string) error {
	n.DataLock.Lock()
	for key, value := range *m {
		n.Data[key] = value
	}
	n.DataLock.Unlock()
	return nil
}

func (n *Mynode) CheckPredecessor_(_ string, _ *string) error {
	n.CheckPredecessor()
	return nil
}

func (n *Mynode) Stabilize_(_ string, _ *string) error {
	n.Stabilize()
	return nil
}

func (n *Mynode) Quit() {
	if !n.Online {
		logrus.Error("Node has already been Quitted ", n.Addr)
		return
	}
	logrus.Info("Node Quit: ", n.Addr)
	var suc, pre string
	_ = n.GetPredecessor(NULL, &pre)
	_ = n.FirstAvailableSuccessor(NULL, &suc)
	n.Close()
	_ = RpcCall(suc, "Mynode.CheckPredecessor_", NULL, nil)
	_ = RpcCall(pre, "Mynode.Stabilize_", NULL, nil)
	n.Reset()
}

func (n *Mynode) ForceQuit() {
	if !n.Online {
		logrus.Error("Node has already been Quitted ", n.Addr)
		return
	}
	logrus.Info("Node ForceQuit: ", n.Addr)
	n.Close()
	n.Reset()
}
