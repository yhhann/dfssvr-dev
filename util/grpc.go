package util

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// UnaryRecoverServerInterceptor is a grpc server-side unary interceptor
// which recover from a grpc panic.
func UnaryRecoverServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			glog.Warningf("%v\n%s", r, getStack())
			err = fmt.Errorf("%v", r)
		}
	}()

	return handler(ctx, req)
}

// StreamRecoverServerInterceptor is a grpc server-side stream interceptor
// which recover from a grpc panic.
func StreamRecoverServerInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			glog.Warningf("%v\n%s", r, getStack())
			err = fmt.Errorf("%v", r)
		}
	}()

	return handler(srv, stream)
}

func getStack() string {
	stack := strings.Split(string(debug.Stack()), "\n")
	stacks := make([]string, 0, len(stack))
	stacks = append(stack[0:1], stack[7:]...)
	return strings.Join(stacks, "\n")
}
