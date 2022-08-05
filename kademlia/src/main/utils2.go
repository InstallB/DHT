package main

import (
	"crypto/sha1"
	"math/big"
	"time"
)

const (
	M                     = 160
	K                     = 20
	alpha                 = 3
	NULL                  = ""
	PingWaitTime          = time.Millisecond * 1000
	MaintainWaitTime      = time.Millisecond * 50
	ExpireRequireTime     = time.Second * 60 * 24
	RepublicIntervalTime  = time.Millisecond * 30000
	BucketRefreshInterval = time.Millisecond * 60000
	FindNodeWaitTime      = time.Millisecond * 20
)

type any interface{}

type Pair struct {
	First  string
	Second string
}

type PairStringId struct {
	First  string
	Second *big.Int
}

type PairListString struct {
	First  []string
	Second string
}

func Prefix(Id *big.Int) int {
	for i := K - 1; i >= 0; i-- {
		if Id.Cmp(pow2((int64)(i))) >= 0 {
			return i
		}
	}
	return -1
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

func Insert(list *[]string, s string, beg int, end int, Id *big.Int) {
	ret := *list
	ret = append(ret, s)
	for i := beg; i < len(ret)-1; i++ {
		if new(big.Int).Xor(GetId(s), Id).Cmp(new(big.Int).Xor(GetId(ret[i]), Id)) < 0 {
			for j := len(ret) - 1; j > i; j-- {
				ret[j] = ret[j-1]
			}
			ret[i] = s
			break
		}
	}
	if end > 0 && len(ret) > end {
		ret = ret[:end]
	}
	*list = ret
}

func GetRand(i int) *big.Int {
	return pow2((int64)(i))
} // generate a random number in [2^i, 2^(i+1))
