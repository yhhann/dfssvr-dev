package notice

import (
	"fmt"
	"testing"
	"time"
)

func TestZkEphemetral(t *testing.T) {
	zk := NewDfsZK([]string{"127.0.0.1:2181"}, 1*time.Second)
	if zk == nil {
		t.Error(" create zk error")
	}
	defer zk.CloseZk()

	sns, errs := zk.CheckChildren("/shard/dfs")

	go func() {
		for {
			select {
			case sn := <-sns:
				for _, s := range sn {
					fmt.Printf("check children : %s\n", s)
				}
			case err := <-errs:
				t.Error(err)
			}
		}
	}()

	ds := []string{"192.168.1.1", "192.168.2.2"}
	for _, d := range ds {
		_, err := zk.createEphemeralSequenceNode("/shard/dfs/n_", []byte(d))
		if err != nil {
			t.Error(err)
		}
	}

	time.Sleep(1 * time.Second)
}
