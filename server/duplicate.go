package server

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context"

	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// Duplicate duplicates a file, returns a new fid.
func (s *DFSServer) Duplicate(ctx context.Context, req *transfer.DuplicateReq) (*transfer.DuplicateRep, error) {
	serviceName := "Duplicate"
	peerAddr := getPeerAddressString(ctx)
	glog.V(3).Infof("%s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Id) == 0 || req.Domain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := bizFunc(s.duplicateBiz).withDeadline(serviceName, ctx, req, peerAddr)
	if err != nil {
		return nil, err
	}

	result, ok := t.(*transfer.DuplicateRep)
	if ok {
		return result, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) duplicateBiz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("parameter number %d", len(args))
	}

	startTime := time.Now()

	peerAddr, ok := args[0].(string)
	req, ok := r.(*transfer.DuplicateReq)
	if !ok {
		return nil, AssertionError
	}

	var rep *transfer.DuplicateRep
	var mf msgFunc
	mf = func() (interface{}, string) {
		return rep, fmt.Sprintf("duplicate, fid %s, domain %d", req.Id, req.Domain)
	}

	did, err := s.duplicate(req.Id, req.Domain)
	if err != nil {
		event := &metadata.Event{
			EType:       metadata.FailDupl,
			Timestamp:   util.GetTimeInMilliSecond(),
			Domain:      req.Domain,
			Fid:         req.Id,
			Description: fmt.Sprintf("%s, client %s", metadata.FailDupl.String(), peerAddr),
			Elapse:      time.Since(startTime).Nanoseconds(),
		}
		if er := s.eventOp.SaveEvent(event); er != nil {
			// log into file instead return.
			glog.Warningf("%s, error: %v", event.String(), er)
		}

		return mf, err
	}

	event := &metadata.Event{
		EType:       metadata.SucDupl,
		Timestamp:   util.GetTimeInMilliSecond(),
		Domain:      req.Domain,
		Fid:         req.Id,
		Description: fmt.Sprintf("%s, client %s, did %s", metadata.SucDupl.String(), peerAddr, did),
		Elapse:      time.Since(startTime).Nanoseconds(),
	}
	if er := s.eventOp.SaveEvent(event); er != nil {
		// log into file instead return.
		glog.Warningf("%s, error: %v", event.String(), er)
	}

	rep = &transfer.DuplicateRep{
		Id: did,
	}
	mf = func() (interface{}, string) {
		return rep, fmt.Sprintf("duplicate new fid %s, fid %s, domain %d", did, req.Id, req.Domain)
	}

	return mf, nil
}

func (s *DFSServer) duplicate(oid string, domain int64) (string, error) {
	h, _, _, err := s.findFileForRead(oid, domain)
	if err != nil {
		return "", err
	}

	// duplicate file from proper handler.
	did, err := h.Duplicate(oid, domain)
	if err != nil {
		return "", err
	}

	return did, nil
}
