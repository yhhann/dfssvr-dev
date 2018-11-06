package server

import (
	"fmt"

	"github.com/golang/glog"
	"golang.org/x/net/context"

	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/proto/transfer"
)

// Exist checks existentiality of a file.
func (s *DFSServer) Exist(ctx context.Context, req *transfer.ExistReq) (result *transfer.ExistRep, err error) {
	serviceName := "Exist"
	peerAddr := getPeerAddressString(ctx)
	defer func() {
		if err != nil {
			glog.Warningf("%s, client: %s, %v, error %v", serviceName, peerAddr, req, err)
		} else {
			glog.V(3).Infof("%s, client: %s, %v, result %t", serviceName, peerAddr, req, result.Result)
		}
	}()

	if len(req.Id) == 0 || req.Domain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	var t interface{}

	if *shieldEnabled {
		bf := func(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
			key := fmt.Sprintf("%s", req.Id)
			return shield(serviceName, key, *shieldTimeout, bizFunc(s.existBiz), c, r, args)
		}

		t, err = bizFunc(bf).withDeadline(serviceName, ctx, req)
	} else {
		t, err = bizFunc(s.existBiz).withDeadline(serviceName, ctx, req)
	}

	if err != nil {
		return nil, err
	}

	ok := false
	if result, ok = t.(*transfer.ExistRep); ok {
		return result, err
	}

	return nil, AssertionError
}

func (s *DFSServer) exist(id string, domain int64) (result bool, err error) {
	defer func() {
		if err == meta.FileNotFound {
			result, err = false, nil
		}
	}()

	_, fid, _, err := s.findFileForRead(id, domain)
	if err != nil {
		return
	}
	if fid == "" {
		return
	}

	result, err = true, nil
	return
}

func (s *DFSServer) existBiz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	req, ok := r.(*transfer.ExistReq)
	if !ok {
		return nil, AssertionError
	}

	result, err := s.exist(req.Id, req.Domain)
	if err != nil {
		glog.V(3).Infof("Failed to exist %s, %d, %v", req.Id, req.Domain, err)
	}

	var mf msgFunc
	mf = func() (interface{}, string) {
		return &transfer.ExistRep{
				Result: result,
			},
			fmt.Sprintf("exist %t, fid %s, domain %d", result, req.Id, req.Domain)
	}

	return mf, err
}
