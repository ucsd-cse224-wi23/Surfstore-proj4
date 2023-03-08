package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	sort "sort"
	"sync"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
	HashList  []string
	mtx       sync.Mutex
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	//hashedId := c.Hash(blockId)
	HashedServer := c.Search(blockId)
	return c.ServerMap[HashedServer]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	c := ConsistentHashRing{
		ServerMap: map[string]string{},
	}
	for _, addr := range serverAddrs {
		hashedAddr := c.Hash(addr)
		log.Println("blockstore name: ", addr)
		log.Println("blockstore hash value: ", hashedAddr)
		c.HashList = append(c.HashList, hashedAddr)
		c.ServerMap[hashedAddr] = addr
	}

	sort.Strings(c.HashList)

	return &c
}

// Binary search over the HashList and return the hash of the server,
// if next server not found, return the hash of the first server
func (c ConsistentHashRing) Search(target string) string {
	//length := len(c.HashList)
	//var left, right int
	//left, right = 0, length-1
	//res := -1
	//for left <= right {
	//	mid := left + (right-left)/2
	//	if c.HashList[mid] == target {
	//		return c.HashList[mid]
	//	} else if c.HashList[mid] < target {
	//		left = mid + 1
	//	} else {
	//		res = mid
	//		right = mid - 1
	//	}
	//}
	//if res == -1 {
	//	return c.HashList[0]
	//} else {
	//	return c.HashList[res]
	//}
	for _, addr := range c.HashList {
		if target > addr {
			continue
		} else {
			return addr
		}
	}
	return c.HashList[0]
}
