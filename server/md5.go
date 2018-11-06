package server

import (
	"fmt"

	"github.com/golang/glog"
	"golang.org/x/net/context"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// GetByMd5 gets a file by its md5.
func (s *DFSServer) GetByMd5(ctx context.Context, req *transfer.GetByMd5Req) (*transfer.GetByMd5Rep, error) {
	serviceName := "GetByMd5"
	peerAddr := getPeerAddressString(ctx)
	glog.V(3).Infof("%s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Md5) == 0 || req.Domain <= 0 || req.Size < 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	var t interface{}
	var err error

	if *shieldEnabled {
		bf := func(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
			key := fmt.Sprintf("%s-%d", req.Md5, req.Domain)
			return shield(serviceName, key, *shieldTimeout, bizFunc(s.getByMd5Biz), c, r, args)
		}

		t, err = bizFunc(bf).withDeadline(serviceName, ctx, req, peerAddr)
	} else {
		t, err = bizFunc(s.getByMd5Biz).withDeadline(serviceName, ctx, req, peerAddr)
	}

	if err != nil {
		return nil, err
	}

	if result, ok := t.(*transfer.GetByMd5Rep); ok {
		return result, nil
	}

	return nil, AssertionError
}

// getByMd5Biz implements an instance of type bizFunc
// to process biz logic for getByMd5.
func (s *DFSServer) getByMd5Biz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("parameter number %d", len(args))
	}
	peerAddr, ok := args[0].(string)
	req, ok := r.(*transfer.GetByMd5Req)
	if !ok {
		return nil, AssertionError
	}

	var rep *transfer.GetByMd5Rep
	var mf msgFunc
	mf = func() (interface{}, string) {
		return rep, fmt.Sprintf("getbymd5, md5 %s, domain %d", req.Md5, req.Domain)
	}

	p, oid, err := s.findByMd5(req.Md5, req.Domain, req.Size)
	if err != nil {
		glog.V(3).Infof("Failed to find file by md5 [%s, %d, %d], error: %v", req.Md5, req.Domain, req.Size, err)
		return mf, err
	}

	did, err := p.Duplicate(oid, req.Domain)
	if err != nil {
		event := &metadata.Event{
			EType:       metadata.FailMd5,
			Timestamp:   util.GetTimeInMilliSecond(),
			Domain:      req.Domain,
			Fid:         oid,
			Description: fmt.Sprintf("%s, client %s", metadata.FailMd5.String(), peerAddr),
		}
		if er := s.eventOp.SaveEvent(event); er != nil {
			// log into file instead return.
			glog.Warningf("%s, error: %v", event.String(), er)
		}

		return mf, err
	}

	event := &metadata.Event{
		EType:       metadata.SucMd5,
		Timestamp:   util.GetTimeInMilliSecond(),
		Domain:      req.Domain,
		Fid:         oid,
		Description: fmt.Sprintf("%s, client %s, did %s", metadata.SucMd5.String(), peerAddr, did),
	}
	if er := s.eventOp.SaveEvent(event); er != nil {
		// log into file instead return.
		glog.Warningf("%s, error: %v", event.String(), er)
	}

	glog.V(3).Infof("Succeeded to get file by md5, fid %v, md5 %v, domain %d, length %d",
		oid, req.Md5, req.Domain, req.Size)

	rep = &transfer.GetByMd5Rep{
		Fid: did,
	}

	mf = func() (interface{}, string) {
		return rep, fmt.Sprintf("getbymd5 new fid %s, md5 %s, domain %d", did, req.Md5, req.Domain)
	}

	return mf, nil
}

// ExistByMd5 checks existentiality of a file.
func (s *DFSServer) ExistByMd5(ctx context.Context, req *transfer.GetByMd5Req) (*transfer.ExistRep, error) {
	serviceName := "ExistByMd5"
	peerAddr := getPeerAddressString(ctx)
	glog.V(3).Infof("%s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Md5) == 0 || req.Domain <= 0 || req.Size < 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	var t interface{}
	var err error

	if *shieldEnabled {
		bf := func(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
			key := fmt.Sprintf("%s-%d", req.Md5, req.Domain)
			return shield(serviceName, key, *shieldTimeout, bizFunc(s.existByMd5Biz), c, r, args)
		}

		t, err = bizFunc(bf).withDeadline(serviceName, ctx, req, peerAddr)
	} else {
		t, err = bizFunc(s.existByMd5Biz).withDeadline(serviceName, ctx, req, peerAddr)
	}

	if err != nil {
		return nil, err
	}

	if result, ok := t.(*transfer.ExistRep); ok {
		return result, nil
	}

	return nil, AssertionError
}

// existByMd5Biz implements an instance of type bizFunc
// to process biz logic for existByMd5.
func (s *DFSServer) existByMd5Biz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	req, ok := r.(*transfer.GetByMd5Req)
	if !ok {
		return nil, AssertionError
	}

	var rep *transfer.ExistRep
	var mf msgFunc
	mf = func() (interface{}, string) {
		return rep, fmt.Sprintf("existbymd5, md5 %s, domain %d", req.Md5, req.Domain)
	}

	_, _, err := s.findByMd5(req.Md5, req.Domain, req.Size)
	if err != nil {
		glog.V(3).Infof("Failed to find file by md5 [%s, %d, %d], error: %v", req.Md5, req.Domain, req.Size, err)
		return mf, err
	}

	rep = &transfer.ExistRep{
		Result: true,
	}
	mf = func() (interface{}, string) {
		return rep, fmt.Sprintf("existbymd5 %t, md5 %s, domain %d", rep.Result, req.Md5, req.Domain)
	}

	return mf, nil
}

func (s *DFSServer) findByMd5(md5 string, domain int64, size int64) (fileop.DFSFileHandler, string, error) {
	var err error
	nh, mh, err := s.selector.getDFSFileHandlerForRead(domain)
	if err != nil {
		glog.Warningf("Failed to get handler for read, error: %v", err)
		return nil, "", err
	}

	var p fileop.DFSFileHandler
	var oid string

	if mh != nil {
		p = *mh
		oid, err = p.FindByMd5(md5, domain, size)
		if err != nil {
			if nh != nil {
				p = *nh
				oid, err = p.FindByMd5(md5, domain, size)
				if err != nil {
					return nil, "", err // Not found in m and n.
				}
			} else {
				return nil, "", meta.FileNotFound // Never reachs this line.
			}
		}
	} else if nh != nil {
		p = *nh
		oid, err = p.FindByMd5(md5, domain, size)
		if err != nil {
			return nil, "", err
		}
	}

	return p, oid, nil
}
