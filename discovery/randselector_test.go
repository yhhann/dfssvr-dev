package discovery

import (
	"fmt"
	"testing"
)

func TestGetPerfectServer(t *testing.T) {
	rs := NewRandSelector()

	rs.AddServer(&DfsServer{Id: "myserver"})
	rs.AddServer(&DfsServer{Id: "yourserver"})
	rs.AddServer(&DfsServer{Id: "hisserver"})
	rs.AddServer(&DfsServer{Id: "herserver"})

	done := make(chan bool, 20)
	for k := 0; k < 20; k++ {
		go func(m int) {
			for i := 0; i < 10; i++ {
				s, err := rs.GetPerfectServer()
				if err != nil {
					t.Errorf("GetPerfectServer error %v", err)
				}
				fmt.Printf("%d:\t%v\n", i+m*10, *s)
			}
			done <- true
		}(k)
	}
	for k := 0; k < 20; k++ {
		<-done
	}
}
