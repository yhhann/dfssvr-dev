package server

import (
	"flag"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/transport"

	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/meta"
)

var (
	logElapseThreshold = flag.Duration("log-elapse-threshold", time.Second, "Elapse threshold for log.")
	logLevelThreshold  = flag.Int("log-level-threshold", 3, "Level threshold for log.")
)

// msgFunc represents function which returns an interface
// and an accompanied message.
type msgFunc func() (interface{}, string)

// bizResult represents the result returns from business function.
type bizResult struct {
	desc string
	r    interface{}
	e    error
}

// streamFunc represents function which process stream operation.
type streamFunc func(interface{}, interface{}, []interface{}) (msgFunc, error)

// withStreamDeadline processes a stream grpc calling with deadline.
func (f streamFunc) withStreamDeadline(serviceName string, req interface{}, stream interface{}, args ...interface{}) error {
	var err error

	defer func() {
		if err != nil {
			err.Error()
		}
	}()

	if grpcStream, ok := stream.(grpc.Stream); ok {
		_, err = bizFunc(streamBizFunc).withDeadline(serviceName, grpcStream, req, f, args)

		return err
	}

	_, err = f(req, stream, args)
	return err
}

// bizFunc represents function which process biz logic.
type bizFunc func(interface{}, interface{}, []interface{}) (interface{}, error)

// withDeadline processes a normal grpc calling with deadline.
func (f bizFunc) withDeadline(serviceName string, env interface{}, req interface{}, args ...interface{}) (r interface{}, e error) {
	startTime := time.Now()

	entry(serviceName)

	msgChan := make(chan string, 1)
	ctx, cancel := context.WithCancel(getContext(env))

	defer func() {
		elapse := time.Since(startTime)
		me := &instrument.Measurements{
			Name:  serviceName,
			Value: float64(elapse.Nanoseconds()),
		}

		if e != nil && cancel != nil {
			cancel()
		}

		e = tryToConvertToStreamError(e)
		if se, ok := e.(transport.StreamError); ok {
			instrument.GrpcErrorByCode <- &instrument.Measurements{
				Name:  fmt.Sprintf("%d", se.Code),
				Value: me.Value,
			}
			glog.Warningf("%s error %v, in %.9f seconds, %s.", serviceName, se, elapse.Seconds(), <-msgChan)
			if se.Code == codes.DeadlineExceeded || se.Code == codes.Canceled {
				instrument.TimeoutHistogram <- me
			}
		} else if e != nil {
			if e == meta.FileNotFound {
				instrument.NotFoundCounter <- me
				glog.Warningf("%s finished in %.9f seconds, not found, %s.", serviceName, elapse.Seconds(), <-msgChan)
			} else {
				instrument.FailedCounter <- me
				glog.Warningf("%s error %v, in %.9f seconds, %s.", serviceName, e, elapse.Seconds(), <-msgChan)
			}
		} else {
			instrument.SuccessDuration <- me
			if elapse > *logElapseThreshold {
				glog.Infof("%s finished in %.9f seconds, %s.", serviceName, elapse.Seconds(), <-msgChan)
			} else {
				glog.V(glog.Level(*logLevelThreshold)).Infof("%s finished in %.9f seconds, %s.", serviceName, elapse.Seconds(), <-msgChan)
			}
		}

		exit(serviceName)
	}()

	result := bizResult{}
	if deadline, ok := ctx.Deadline(); ok {
		timeout := deadline.Sub(startTime)

		if timeout <= 0 {
			glog.V(3).Infof("%s timeout is %v, deadline is %v", serviceName, timeout, deadline)
			e = context.DeadlineExceeded
			return
		}

		results := make(chan *bizResult, 1)

		go func() {
			// Do business.
			result = callBizFunc(f, env, req, args)
			results <- &result
			close(results)
		}()

		select {
		case result := <-results:
			r = result.r
			e = result.e
			msgChan <- result.desc
			return
		case <-ctx.Done():
			e = ctx.Err()
			msgChan <- e.Error()
			return
		}
	}

	instrument.NoDeadlineCounter <- &instrument.Measurements{
		Name:  serviceName,
		Value: 1,
	}

	result = callBizFunc(f, env, req, args)
	msgChan <- result.desc
	return result.r, result.e
}

func callBizFunc(f bizFunc, env interface{}, req interface{}, args []interface{}) (result bizResult) {
	defer func() {
		if r := recover(); r != nil {
			glog.Warningf("Recovered from %v\n%s", r, getStack())
			result.e = fmt.Errorf("%v", r)
		}
	}()

	var err error
	result.r, err = f(env, req, args)
	if err != nil {
		result.e = err
	}

	if mf, ok := result.r.(msgFunc); ok && mf != nil {
		result.r, result.desc = mf()
	}

	return
}

// streamBizFunc is an instance of bizFunc.
func streamBizFunc(stream interface{}, req interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("parameter number %d", len(args))
	}

	sFunc, ok := args[0].(streamFunc)
	if !ok {
		return nil, AssertionError
	}
	as, ok := args[1].([]interface{})
	if !ok {
		return nil, AssertionError
	}

	return sFunc(req, stream, as)
}

func getDeadline(env interface{}) (time.Time, bool) {
	return getContext(env).Deadline()
}

func getContext(env interface{}) (ctx context.Context) {
	switch t := env.(type) {
	case context.Context:
		ctx = t
	case grpc.Stream:
		ctx = t.Context()
	default:
		ctx = context.Background()
	}

	return
}

func checkTimeout(size int64, rate float64, given time.Duration) (time.Duration, error) {
	if rate != 0.0 && size != 0 {
		rate := float64(rate * 1024) // convert unit of rate from kbit/s to bit/s
		size := float64(size * 8)    // convert unit of size from bytes to bits
		need := time.Duration(size / rate * float64(time.Second))
		if given.Nanoseconds() < need.Nanoseconds() {
			return time.Duration(need), context.DeadlineExceeded
		}
	}

	return given, nil
}

func getStack() string {
	stack := strings.Split(string(debug.Stack()), "\n")
	stacks := make([]string, 0, len(stack))
	stacks = append(stack[0:1], stack[7:]...)
	return strings.Join(stacks, "\n")
}

func tryToConvertToStreamError(err error) error {
	switch err {
	case context.DeadlineExceeded:
		return transport.StreamError{
			Code: codes.DeadlineExceeded,
			Desc: fmt.Sprintf("%v", err),
		}
	case context.Canceled:
		return transport.StreamError{
			Code: codes.Canceled,
			Desc: fmt.Sprintf("%v", err),
		}
	default:
		return err
	}
}
