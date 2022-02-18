package consisthash

import (
	"fmt"
	"testing"
	"time"
)

func TestConsistHash_AddNode(t *testing.T) {
	ch := New()
	nodeStr := "node"
	nodeSum := 32
	virtualNodeSum := int32(102400)
	for i := 0; i < nodeSum; i++ {
		addr := fmt.Sprintf("%s_%d", nodeStr, i)
		ch.AddNode(addr, virtualNodeSum)
		// 校验node，virtualNode
		node := ch.trueNodeMap[addr]
		if node.addr != addr || node.virtualCount != virtualNodeSum {
			t.Fatalf("true node=%v err", node)
		}
		if len(ch.sortedVirtual) != (i+1)*int(virtualNodeSum) {
			//t.Fatalf("virtualNodeSum:%d != i*virtualNodeSum:%d", len(ch.sortedVirtual), (i+1)*int(virtualNodeSum))
		}
		t.Logf("virtualNodeSum=%d", len(ch.sortedVirtual))
	}
}

func TestBreak(t *testing.T) {
	tick := time.Tick(time.Second)
	for {
		fmt.Printf("outside select\n")
		select {
		case t := <-tick:
			fmt.Println(t)
			continue
			fmt.Println("test")
		}
	}
	fmt.Println("end")
}
