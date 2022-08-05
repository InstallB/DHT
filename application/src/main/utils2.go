package main

import (
	"crypto/sha1"
	"math/big"
	"time"
)

const (
	M                   = 160
	SuccessorListLength = 5
	NULL                = ""
	PingWaitTime        = time.Millisecond * 1000
	MapWaitTime         = time.Millisecond * 100
	MaintainWaitTime    = time.Millisecond * 100
)

type any interface{}

type Pair struct {
	First  string
	Second string
}

type Pair2 struct {
	First  string
	Second int
}

func pow2(exp int64) *big.Int {
	return new(big.Int).Exp(big.NewInt(2), big.NewInt(exp), nil)
}

func GetId(addr string) *big.Int {
	val := sha1.New()
	ret := new(big.Int)
	ret.SetBytes(val.Sum([]byte(addr)))
	return ret
} // hash

func InRange(x *big.Int, l *big.Int, r *big.Int) bool {
	if l.Cmp(r) <= 0 {
		return l.Cmp(x) <= 0 && x.Cmp(r) <= 0
		// return l <= x && x <= r
	} else {
		return l.Cmp(x) <= 0 || x.Cmp(r) <= 0
		// return x >= l || x <= r
	}
}

//func InRange(x *big.Int, l *big.Int, r *big.Int, RightClosed bool) bool {
//	if RightClosed == false {
//		if l.Cmp(r) <= 0 {
//			return l.Cmp(x) < 0 && x.Cmp(r) < 0
//			// return l < x && x < r
//		} else {
//			return l.Cmp(x) < 0 || x.Cmp(r) < 0
//			// return x > l || x < r
//		}
//	} else {
//		if l.Cmp(r) <= 0 {
//			return l.Cmp(x) < 0 && x.Cmp(r) <= 0
//			// return l < x && x <= r
//		} else {
//			return l.Cmp(x) < 0 || x.Cmp(r) <= 0
//			// return x > l || x <= r
//		}
//	}
//} // whether x is in (l,r) (RightClosed = false) / (l,r] (RightClosed = true)
