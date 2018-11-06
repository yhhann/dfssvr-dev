package server

import (
	"flag"
	"fmt"

	"github.com/golang/glog"
	"golang.org/x/net/context"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/proto/transfer"
)

const (
	MaxChunkSize = 1048576 // Max chunk size in bytes.
	MinChunkSize = 1024    // Min chunk size in bytes.
)

var (
	DefaultChunkSizeInBytes = flag.Int64("default-chunk-size", 1048576, "default chunk size in bytes.")
)

// NegotiateChunkSize negotiates chunk size in bytes between client and server.
func (s *DFSServer) NegotiateChunkSize(ctx context.Context, req *transfer.NegotiateChunkSizeReq) (*transfer.NegotiateChunkSizeRep, error) {
	serviceName := "NegotiateChunkSize"
	peerAddr := getPeerAddressString(ctx)
	glog.V(3).Infof("%s, client: %s, %v", serviceName, peerAddr, req)

	t, err := bizFunc(s.negotiateBiz).withDeadline("NegotiateChunkSize", ctx, req)
	if err != nil {
		return nil, err
	}

	if rep, ok := t.(*transfer.NegotiateChunkSizeRep); ok {
		return rep, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) negotiateBiz(ctx interface{}, req interface{}, args []interface{}) (interface{}, error) {
	if r, ok := req.(*transfer.NegotiateChunkSizeReq); ok {
		fileop.NegotiatedChunkSize = sanitizeChunkSize(r.Size)
		rep := &transfer.NegotiateChunkSizeRep{
			Size: fileop.NegotiatedChunkSize,
		}

		var mf msgFunc
		mf = func() (interface{}, string) {
			return rep, fmt.Sprintf("negosize size %d", rep.Size)
		}

		return mf, nil

	}
	return nil, AssertionError
}

func sanitizeChunkSize(size int64) int64 {
	if size < MinChunkSize {
		return MinChunkSize
	}
	if size > MaxChunkSize {
		return MaxChunkSize
	}
	return size
}
