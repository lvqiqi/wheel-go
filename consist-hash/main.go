package consisthash

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

type node struct {
	addr         string
	virtualCount int32
}

type virtualNode struct {
	node *node
	hash uint32
}

// ConsistHash represents a consist hash.
type ConsistHash struct {
	trueNodeMap   map[string]*node
	sortedVirtual []virtualNode
}

// New new a ConsistHash instance.
func New() *ConsistHash {
	trueNodeMap := make(map[string]*node)
	sortedVirtual := make([]virtualNode, 0)
	return &ConsistHash{trueNodeMap: trueNodeMap, sortedVirtual: sortedVirtual}
}

// GetNode calculate key hash and return a true node's addr.
func (c *ConsistHash) GetNode(key string) string {
	h := c.hash(key)
	index := sort.Search(len(c.sortedVirtual), func(i int) bool {
		return c.sortedVirtual[i].hash >= h
	})
	if index == len(c.sortedVirtual) {
		index = 0
	}
	return c.sortedVirtual[index].node.addr

}

// AddNode accepts a addr and a virtualCount.
func (c *ConsistHash) AddNode(addr string, virtualCount int32) error {
	node := &node{addr: addr, virtualCount: virtualCount}
	if c.trueNodeMap[addr] != nil {
		return errors.New("addr:" + addr + "already exists")
	}
	c.trueNodeMap[addr] = node
	for i := int32(0); i < virtualCount; i++ {
		virtualNodeAddr := fmt.Sprintf("%s_%d", addr, i)
		h := c.hash(virtualNodeAddr)
		virtualNode := virtualNode{
			node: node,
			hash: h,
		}
		c.sortedVirtual = append(c.sortedVirtual, virtualNode)
	}
	sort.Slice(c.sortedVirtual, func(i, j int) bool {
		return c.sortedVirtual[i].hash < c.sortedVirtual[j].hash
	})
	index := c.removeDuplicates()
	c.sortedVirtual = c.sortedVirtual[:index]
	return nil
}

// RemoveNode remove the node of the addr.
func (c *ConsistHash) RemoveNode(addr string) error {
	tmp := make([]virtualNode, 0, 0)
	for i := range c.sortedVirtual {
		if c.sortedVirtual[i].node.addr != addr {
			tmp = append(tmp, c.sortedVirtual[i])
		}
	}
	c.sortedVirtual = tmp
	delete(c.trueNodeMap, addr)
	return nil
}

func (c *ConsistHash) removeDuplicates() int {
	n := len(c.sortedVirtual)
	if n == 0 {
		return 0
	}
	slow := 0
	fast := 1
	for fast < n {
		if c.sortedVirtual[slow].hash != c.sortedVirtual[fast].hash {
			slow++
			c.sortedVirtual[slow] = c.sortedVirtual[fast]
		} else {
			fmt.Printf("have same hash c.sortedVirtual[slow]=%v c.sortedVirtual[fast]=%v",
				c.sortedVirtual[slow], c.sortedVirtual[fast])
		}
		fast++
	}
	return slow + 1
}

func (c *ConsistHash) hash(key string) uint32 {
	digest := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(digest[0:])
}
