package server

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestShield(t *testing.T) {
	var wg sync.WaitGroup

	cnt := 10
	icnt := 1000
	wg.Add(cnt * icnt)
	for i := 0; i < cnt; i++ {

		for j := 0; j < icnt; j++ {
			//time.Sleep(time.Duration(rand.Intn(1)) * time.Millisecond)
			key := fmt.Sprintf("ok%d", j)
			go func(m int) {
				result, err := shield(key, 2*time.Second,

					func(a interface{}, b interface{}, c []interface{}) (interface{}, error) {
						s := fmt.Sprintf("%v-%v", a, b)
						fmt.Printf("I am in shielded function, %s\n", s)
						time.Sleep(time.Duration(rand.Intn(1300)) * time.Millisecond)
						return s, nil
					}, key, m, 0.01, true)

				fmt.Printf("result: %+v, error : %v\n", result, err)
				wg.Done()
			}(i)
		}

	}

	wg.Wait()
}
