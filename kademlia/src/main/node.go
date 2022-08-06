package main

import (
	"github.com/sirupsen/logrus"
	"math/big"
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
	var i Node
	i.Initialize(GetLocalAddress() + ":" + strconv.Itoa(port))
	var a DhtNode = &i
	return a
}

type Node struct {
	Addr string

	Bucket            [M][K]string
	BucketRefreshTime [M]time.Time
	BucketTimeLock    sync.RWMutex
	BucketLock        sync.RWMutex

	DataBase DataBase

	Online   bool
	Server   *rpc.Server
	Listener net.Listener

	QuitSignal chan bool
}

func (n *Node) Initialize(addr string) {
	logrus.Info("Initialize: ", addr)
	n.Online = false
	n.Addr = addr
	n.Server = rpc.NewServer()
	n.QuitSignal = make(chan bool, 2)
	n.DataBase.Init()
	for i := 0; i < M; i++ {
		for j := 0; j < K; j++ {
			n.Bucket[i][j] = NULL
		}
	}
}

func (n *Node) Reset() {
	logrus.Info("Node Reset: ", n.Addr)
	n.Online = false
	n.Addr = NULL
	n.QuitSignal = make(chan bool, 2)
	n.DataBase.Init()
	for i := 0; i < M; i++ {
		for j := 0; j < K; j++ {
			n.Bucket[i][j] = NULL
		}
	}
	err := n.Listener.Close()
	if err != nil {
		logrus.Error("Listener Close Failed in Reset")
		return
	}
}

func (n *Node) InitializeServer() {
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

func (n *Node) Run() {
	n.InitializeServer()
	n.Maintain()
}

func (n *Node) Create() {
	logrus.Info(n.Addr, " Create New Network")
	n.Online = true
}

func (n *Node) Ping(addr string) bool {
	ret, err := Ping(addr)
	if err != nil {
		return false
	}
	return ret
}

func (n *Node) GetClosestNodes(Id *big.Int, ret *[]string) {
	BucketId := Prefix(new(big.Int).Xor(GetId(n.Addr), Id))
	list := make([]string, 0, K)
	n.BucketLock.RLock()

	if BucketId != -1 {
		for i := K - 1; i >= 0 && len(list) < K; i-- {
			if n.Bucket[BucketId][i] != NULL {
				list = append(list, n.Bucket[BucketId][i])
			}
		}
	}

	for i := 1; (BucketId-i >= 0 || BucketId+i < M) && len(list) < K; i++ {
		if BucketId-i >= 0 {
			for j := K - 1; j >= 0 && len(list) < K; j-- {
				if n.Bucket[BucketId-i][j] != NULL {
					list = append(list, n.Bucket[BucketId-i][j])
				}
			}
		}
		if BucketId+i < M {
			for j := K - 1; j >= 0 && len(list) < K; j-- {
				if n.Bucket[BucketId+i][j] != NULL {
					list = append(list, n.Bucket[BucketId+i][j])
				}
			}
		}
	}
	*ret = list
	n.BucketLock.RUnlock()
}

func (n *Node) GetClose(pi PairStringId, ret *[]string) error {
	list := make([]string, 0, K)
	n.UpdateBucket(pi.First)
	n.GetClosestNodes(pi.Second, &list)
	*ret = list
	return nil
}

func (n *Node) FindNode(Id *big.Int, ret *[]string) error {
	list := make([]string, 0, K) // list of nodes found but haven't run a FindNode
	tmplist := make([]string, 0, K)
	result := make([]string, 0, K+1) // list of already responded nodes
	visited := make(map[string]bool)
	inlist := make(map[string]bool)
	visited[n.Addr] = true
	inlist[n.Addr] = true
	n.GetClosestNodes(Id, &tmplist)
	for i := 0; i < len(tmplist); i++ {
		if tmplist[i] != n.Addr {
			Insert(&list, tmplist[i], 0, -1, Id)
		}
	}
	ch := make(chan []string, alpha)
	InRun := 0

	for i := 0; i < alpha; i++ {
		if i >= len(list) {
			break
		}
		visited[list[i]] = true
		inlist[list[i]] = true
		InRun++
		go func(dest string, channel chan []string, ir *int) {
			res := make([]string, 0, K+1)
			err := RpcCall(dest, "Node.GetClose", PairStringId{n.Addr, Id}, &res)
			res = append(res, dest)
			if err != nil {
				*ir--
				logrus.Info(dest, " GetClose Failed in FindNode[", n.Addr, "]")
				return
			}
			channel <- res
		}(list[i], ch, &InRun)
	}

	index := 0
	for {
		if InRun > 0 {
			select {
			case res := <-ch:
				tmp := res[len(res)-1]
				Insert(&result, tmp, 0, K, Id)
				n.UpdateBucket(tmp)
				InRun--
				for i := 0; i < len(res)-1; i++ {
					if _, ok := inlist[res[i]]; !ok {
						Insert(&list, res[i], index, -1, Id)
						inlist[res[i]] = true
						list = append(list, res[i])
					}
				}
			case <-time.After(FindNodeWaitTime):
				// Time Out
			}
		}
		if InRun < alpha && index < len(list) {
			tmp := list[index]
			_, ok := visited[tmp]
			if !ok {
				visited[tmp] = true
				InRun++
				go func(dest string, channel chan []string, ir *int) {
					res := make([]string, 0, K+1)
					err := RpcCall(dest, "Node.GetClose", PairStringId{n.Addr, Id}, &res)
					res = append(res, dest)
					if err != nil {
						*ir--
						logrus.Info(dest, " GetClose Failed in FindNode[", n.Addr, "]")
						return
					}
					channel <- res
				}(tmp, ch, &InRun)
			}
			index++
		}
		if index >= len(list) && InRun == 0 {
			break
		}
		if index < len(list) {
			if len(result) == K && new(big.Int).Xor(GetId(result[len(result)-1]), Id).Cmp(new(big.Int).Xor(GetId(list[index]), Id)) < 0 {
				break
			}
		}
	}
	*ret = result
	return nil
}

func (n *Node) UpdateBucket(addr string) { // addr enters n's k-bucket
	if n.Addr == addr || addr == NULL {
		return
	}
	BucketId := Prefix(new(big.Int).Xor(GetId(n.Addr), GetId(addr)))
	n.BucketTimeLock.Lock()
	n.BucketRefreshTime[BucketId] = time.Now().Add(BucketRefreshInterval)
	n.BucketTimeLock.Unlock()
	pos := -1
	n.BucketLock.Lock()
	for i := 0; i < K; i++ {
		if n.Bucket[BucketId][i] == addr {
			pos = i
			break
		}
	}
	if pos == -1 {
		for i := 0; i < K; i++ {
			if n.Bucket[BucketId][i] == NULL {
				n.Bucket[BucketId][i] = addr
				n.BucketLock.Unlock()
				return
			}
		}
		PingResult := n.Ping(n.Bucket[BucketId][0])
		if PingResult {
			tmp := n.Bucket[BucketId][0]
			for j := 1; j < K; j++ {
				n.Bucket[BucketId][j-1] = n.Bucket[BucketId][j]
			}
			n.Bucket[BucketId][K-1] = tmp
		} else {
			for j := 1; j < K; j++ {
				n.Bucket[BucketId][j-1] = n.Bucket[BucketId][j]
			}
			n.Bucket[BucketId][K-1] = addr
		}
	} else {
		for i := pos; i < K-1; i++ {
			n.Bucket[BucketId][i] = n.Bucket[BucketId][i+1]
		}
		n.Bucket[BucketId][K-1] = NULL
		for i := pos; i < K; i++ {
			if n.Bucket[BucketId][i] == NULL {
				n.Bucket[BucketId][i] = addr
				break
			}
		}
	}
	n.BucketLock.Unlock()
	return
}

func (n *Node) Join(addr string) bool {
	logrus.Info(n.Addr, " Node Join through ", addr)
	n.UpdateBucket(addr)
	res := make([]string, 0, K)
	_ = n.FindNode(GetId(n.Addr), &res)
	BucketId := Prefix(new(big.Int).Xor(GetId(n.Addr), GetId(addr)))
	for i := 0; i < BucketId; i++ {
		n.BucketTimeLock.Lock()
		n.BucketRefreshTime[i] = time.Now().Add(BucketRefreshInterval)
		n.BucketTimeLock.Unlock()
	}
	for i := BucketId + 1; i < M; i++ {
		n.RefreshBucket(i)
	}
	n.Online = true
	return true
}

func (n *Node) PutData(kv Pair, _ *string) error {
	n.DataBase.Store(kv)
	return nil
}

func (n *Node) RangePut(kv Pair) {
	Id := GetId(kv.First)
	list := make([]string, 0, K)
	_ = n.FindNode(Id, &list)
	logrus.Info(n.Addr, " RangePut ", kv.First, list)
	for i := 0; i < len(list); i++ {
		err := RpcCall(list[i], "Node.PutData", kv, nil)
		if err != nil {
			logrus.Error(n.Addr, ",", list[i], " PutData Failed in RangePut")
			return
		}
	}
}

func (n *Node) Put(key string, value string) bool {
	logrus.Info(n.Addr, " Put ", key)
	n.RangePut(Pair{key, value})
	_ = n.PutData(Pair{key, value}, nil)
	return true
}

func (n *Node) GetCloseData(pi Pair, ret *PairListString) error {
	list := make([]string, 0, K)
	n.UpdateBucket(pi.First)
	n.GetClosestNodes(GetId(pi.Second), &list)
	_, value := n.DataBase.Get(pi.Second)
	*ret = PairListString{list, value}
	return nil
}

func (n *Node) GetData(key string) (bool, string) {
	if ok, value := n.DataBase.Get(key); ok {
		return true, value
	}
	Id := GetId(key)
	list := make([]string, 0, K) // list of nodes found but haven't run a FindNode
	tmplist := make([]string, 0, K)
	result := make([]string, 0, K+1) // list of already responded nodes
	visited := make(map[string]bool)
	inlist := make(map[string]bool)
	visited[n.Addr] = true
	inlist[n.Addr] = true
	n.GetClosestNodes(Id, &tmplist)
	for i := 0; i < len(tmplist); i++ {
		if tmplist[i] != n.Addr {
			Insert(&list, tmplist[i], 0, -1, Id)
		}
	}
	ch := make(chan []string, alpha)
	chdata := make(chan Pair, 2)
	InRun := 0

	for i := 0; i < alpha; i++ {
		if i >= len(list) {
			break
		}
		visited[list[i]] = true
		inlist[list[i]] = true
		InRun++
		go func(dest string, channel chan []string, ir *int) {
			res := PairListString{make([]string, 0, K+1), NULL}
			err := RpcCall(dest, "Node.GetCloseData", Pair{n.Addr, key}, &res)
			res.First = append(res.First, dest)
			if err != nil {
				*ir--
				logrus.Info(dest, " GetCloseData Failed in GetData[", n.Addr, "]")
				return
			}
			if res.Second != NULL {
				chdata <- Pair{res.Second, dest}
			}
			channel <- res.First
		}(list[i], ch, &InRun)
	}

	index := 0
	for {
		if InRun > 0 {
			select {
			case res := <-chdata:
				n.UpdateBucket(res.Second)
				return true, res.First
			case res := <-ch:
				tmp := res[len(res)-1]
				Insert(&result, tmp, 0, K, Id)
				n.UpdateBucket(tmp)
				InRun--
				for i := 0; i < len(res)-1; i++ {
					if _, ok := inlist[res[i]]; !ok {
						Insert(&list, res[i], index, -1, Id)
						inlist[res[i]] = true
						list = append(list, res[i])
					}
				}
			case <-time.After(FindNodeWaitTime):
				// Time Out
			}
		}
		if InRun < alpha && index < len(list) {
			tmp := list[index]
			_, ok := visited[tmp]
			if !ok {
				visited[tmp] = true
				InRun++
				go func(dest string, channel chan []string, ir *int) {
					res := PairListString{make([]string, 0, K+1), NULL}
					err := RpcCall(dest, "Node.GetCloseData", Pair{n.Addr, key}, &res)
					res.First = append(res.First, dest)
					if err != nil {
						*ir--
						logrus.Info(dest, " GetCloseData Failed in GetData[", n.Addr, "]")
						return
					}
					if res.Second != NULL {
						chdata <- Pair{res.Second, dest}
					}
					channel <- res.First
				}(tmp, ch, &InRun)
			}
			index++
		}
		if index >= len(list) && InRun == 0 {
			break
		}
		if index < len(list) {
			if len(result) == K && new(big.Int).Xor(GetId(result[len(result)-1]), Id).Cmp(new(big.Int).Xor(GetId(list[index]), Id)) < 0 {
				break
			}
		}
	}
	return false, NULL
}

func (n *Node) Get(key string) (bool, string) {
	return n.GetData(key)
}

func (n *Node) Delete(_ string) bool {
	return true
}

func (n *Node) Republic() {
	m := make(map[string]string)
	n.DataBase.GetRepublicList(&m)
	for key, value := range m {
		if !n.Online {
			return
		}
		n.RangePut(Pair{key, value})
	}
}

func (n *Node) RefreshBucket(i int) {
	Id := new(big.Int).Xor(GetId(n.Addr), GetRand(i))
	list := make([]string, 0, K)
	_ = n.FindNode(Id, &list)
}

func (n *Node) Refresh() {
	for i := 0; i < M; i++ {
		if !n.Online {
			return
		}
		n.BucketTimeLock.RLock()
		if !n.BucketRefreshTime[i].After(time.Now()) {
			n.BucketTimeLock.RUnlock()
			n.RefreshBucket(i)
			n.BucketTimeLock.Lock()
			n.BucketRefreshTime[i] = time.Now().Add(BucketRefreshInterval)
			n.BucketTimeLock.Unlock()
		} else {
			n.BucketTimeLock.RUnlock()
		}
	}
}

func (n *Node) Maintain() {
	go func() {
		for {
			if n.Online {
				n.Refresh()
			}
			time.Sleep(MaintainWaitTime)
		}
	}()
	go func() {
		for {
			if n.Online {
				n.Republic()
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()
	go func() {
		for {
			if n.Online {
				n.DataBase.Expire(n.Addr)
			}
			time.Sleep(time.Millisecond * 1000)
		}
	}()
}

func (n *Node) Close() {
	n.QuitSignal <- true
}

func (n *Node) Quit() {
	logrus.Error("Node Quit: ", n.Addr)
	n.Close()
	n.Reset()
}

func (n *Node) ForceQuit() {
	logrus.Error("Node ForceQuit: ", n.Addr)
	n.Close()
	n.Reset()
}
