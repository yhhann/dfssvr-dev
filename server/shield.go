package server

import (
	"context"
	"errors"
	"flag"
	"sync"
	"time"

	"github.com/golang/glog"

	"jingoal.com/dfs/instrument"
)

var (
	shieldTimeout = flag.Duration("shield-timeout", 60*time.Second, "shield timeout duration.")
	shieldEnabled = flag.Bool("shield-enabled", false, "enable shield")

	tasks    map[string][]chan BizFuncResult
	mapLock  sync.Mutex
	taskLock sync.Mutex
)

type BizFuncResult struct {
	r interface{}
	e error
}

func getWithLock(key string) ([]chan BizFuncResult, bool) {
	mapLock.Lock()
	defer mapLock.Unlock()

	r, ok := tasks[key]
	return r, ok
}

func setWithLock(key string, v []chan BizFuncResult) {
	mapLock.Lock()
	defer mapLock.Unlock()

	tasks[key] = v
}

func delWithLock(key string) {
	mapLock.Lock()
	defer mapLock.Unlock()

	delete(tasks, key)
}

func put(key string, c chan BizFuncResult) bool {
	taskLock.Lock()
	defer taskLock.Unlock()

	tmp, ok := getWithLock(key)
	if !ok {
		tmp = make([]chan BizFuncResult, 0, 10)
	}

	setWithLock(key, append(tmp, c))

	return !ok
}

func poll(key string) chan chan BizFuncResult {
	ch := make(chan chan BizFuncResult, 100)

	go func() {
		tmp, ok := getWithLock(key)
		if !ok {
			close(ch)
		}

		for _, c := range tmp {
			ch <- c
		}

		delWithLock(key)
		close(ch)
	}()

	return ch
}

func shield(serviceName string, key string, timeout time.Duration, f bizFunc, paras ...interface{}) (interface{}, error) {
	if len(paras) < 2 {
		glog.Errorf("parameter number less than 2.")
		return nil, errors.New("parameter number less than 2")
	}

	tckr := time.NewTicker(timeout)

	task := make(chan BizFuncResult, 1)

	first := put(key, task)
	if first {
		start := time.Now()
		rslt := BizFuncResult{}
		rslt.r, rslt.e = f(paras[0], paras[1], paras[2:])

		cnt := 0
		for t := range poll(key) {
			t <- rslt
			cnt++
		}

		if cnt > 1 {
			instrument.MergedQuery <- &instrument.Measurements{
				Name:  serviceName,
				Value: float64(cnt),
			}
			glog.V(3).Infof("Succeeded to merge req: %d, %s, %s, elapse %v", cnt, serviceName, key, time.Since(start))
		}
	}

	select {
	case result := <-task:
		close(task)
		return result.r, result.e
	case <-tckr.C:
		return nil, context.DeadlineExceeded
	}
}

func init() {
	tasks = make(map[string][]chan BizFuncResult)
}
