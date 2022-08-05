package main

import (
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type DataBase struct {
	DataLock     sync.RWMutex
	Data         map[string]string
	RepublicTime map[string]time.Time
	ExpireTime   map[string]time.Time // store the time when key is expected to expire
}

func (db *DataBase) Init() {
	db.Data = make(map[string]string)
	db.RepublicTime = make(map[string]time.Time)
	db.ExpireTime = make(map[string]time.Time)
}

func (db *DataBase) Store(kv Pair) {
	db.DataLock.Lock()
	defer db.DataLock.Unlock()
	key := kv.First
	_, ok := db.Data[key]
	if !ok {
		db.Data[key] = kv.Second
	}
	db.RepublicTime[key] = time.Now().Add(RepublicIntervalTime)
	db.ExpireTime[key] = time.Now().Add(ExpireRequireTime)
}

func (db *DataBase) Get(key string) (bool, string) {
	db.DataLock.Lock()
	defer db.DataLock.Unlock()
	value, ok := db.Data[key]
	if ok {
		db.ExpireTime[key] = time.Now().Add(ExpireRequireTime)
		return true, value
	} else {
		return false, NULL
	}
}

func (db *DataBase) GetRepublicList(ret *map[string]string) {
	rep := make(map[string]time.Time)
	db.DataLock.Lock()
	defer db.DataLock.Unlock()
	for key, value := range db.RepublicTime {
		if !value.After(time.Now()) {
			rep[key] = value
		}
	}
	for key, value := range rep {
		(*ret)[key] = db.Data[key]
		db.RepublicTime[key] = value.Add(RepublicIntervalTime)
	}
}

func (db *DataBase) Expire(addr string) {
	del := make(map[string]bool)
	db.DataLock.Lock()
	defer db.DataLock.Unlock()
	for key, value := range db.ExpireTime {
		if !value.After(time.Now()) {
			del[key] = true
		}
	}
	for key := range del {
		logrus.Error(addr, " Expires Key ", key)
		delete(db.Data, key)
		delete(db.RepublicTime, key)
		delete(db.ExpireTime, key)
	}
}
