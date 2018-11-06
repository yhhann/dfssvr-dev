package discovery

import (
	"fmt"
	"testing"
	"time"
)

func TestRegister(t *testing.T) {
	r := NewZKDfsServerRegister("127.0.0.1:2181", 1*time.Second)

	s := DfsServer{Id: "myServer"}

	if err := r.Register(&s); err != nil {
		t.Errorf("register error %v", err)
	}

	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Second)
		for _, v := range r.GetDfsServerMap() {
			fmt.Printf("%v\n", v)
		}
	}

}

func TestObserver(t *testing.T) {
	r := NewZKDfsServerRegister("127.0.0.1:2181", 1*time.Second)
	s := DfsServer{Id: "yourServer"}
	if err := r.Register(&s); err != nil {
		t.Errorf("register error %v", err)
	}

	client := make(chan struct{})
	r.AddObserver(client, "")

	go func() {
		for v := range client {
			fmt.Printf("action, %+v\n", v)
		}

	}()

	r.putDfsServerToMap(&DfsServer{})
	r.putDfsServerToMap(&DfsServer{})
	r.RemoveObserver(client)
	r.putDfsServerToMap(&DfsServer{})
	r.putDfsServerToMap(&DfsServer{})

	time.Sleep(1 * time.Second)
}
