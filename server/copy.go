package server

import (
	"fmt"
	"io"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context"

	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// Copy copies a file and returns its fid.
func (s *DFSServer) Copy(ctx context.Context, req *transfer.CopyReq) (*transfer.CopyRep, error) {
	serviceName := "Copy"
	peerAddr := getPeerAddressString(ctx)
	glog.V(3).Infof("%s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.SrcFid) == 0 || req.SrcDomain <= 0 || req.DstDomain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := bizFunc(s.copyBiz).withDeadline(serviceName, ctx, req, peerAddr)
	if err != nil {
		return nil, err
	}

	result, ok := t.(*transfer.CopyRep)
	if ok {
		return result, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) copyBiz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("parameter number %d", len(args))
	}
	peerAddr, ok := args[0].(string)
	req, ok := r.(*transfer.CopyReq)
	if !ok {
		return nil, AssertionError
	}

	var rep *transfer.CopyRep
	var mf msgFunc
	mf = func() (interface{}, string) {
		return rep, fmt.Sprintf("copy, srcfid %s, srddomain %d, dstdomain %d", req.SrcFid, req.SrcDomain, req.DstDomain)
	}

	if req.SrcDomain == req.DstDomain {
		did, err := s.duplicate(req.SrcFid, req.DstDomain)
		if err != nil {
			return mf, err
		}

		glog.V(3).Infof("Copy is converted to duplicate, srcId: %s, srcDomain: %d, dstDomain: %d",
			req.SrcFid, req.SrcDomain, req.DstDomain)

		rep = &transfer.CopyRep{
			Fid: did,
		}
		mf = func() (interface{}, string) {
			return rep, fmt.Sprintf("copy new fid %s, srcfid %s, srddomain %d, dstdomain %d", did, req.SrcFid, req.SrcDomain, req.DstDomain)
		}

		return mf, nil
	}

	startTime := time.Now()

	_, rf, err := s.openFileForRead(req.SrcFid, req.SrcDomain)
	if err != nil {
		return mf, err
	}
	defer rf.Close()

	// open destination file.
	handler, err := s.selector.getDFSFileHandlerForWrite(req.DstDomain)
	if err != nil {
		return mf, err
	}

	copiedInf := *rf.GetFileInfo()
	copiedInf.Domain = req.DstDomain
	copiedInf.User = req.DstUid
	copiedInf.Biz = req.DstBiz

	wf, err := (*handler).Create(&copiedInf)
	if err != nil {
		return mf, err
	}

	defer wf.Close()

	length, err := io.Copy(wf, rf)
	if err != nil {
		glog.Warningf("Failed to copy file %s, %v", req.SrcFid, err)
		return mf, err
	}

	inf := wf.GetFileInfo()
	glog.V(3).Infof("Succeeded to copy file %s to %s", req.SrcFid, inf.Id)

	// space log.
	slog := &metadata.SpaceLog{
		Domain:    inf.Domain,
		Uid:       fmt.Sprintf("%d", inf.User),
		Fid:       inf.Id,
		Biz:       inf.Biz,
		Size:      length,
		Timestamp: time.Now(),
		Type:      metadata.CreateType.String(),
	}
	err = s.spaceOp.SaveSpaceLog(slog)
	if err != nil {
		glog.Warningf("%s, error: %v", slog.String(), err)
	}

	event := &metadata.Event{
		EType:     metadata.SucCreate,
		Timestamp: util.GetTimeInMilliSecond(),
		Domain:    inf.Domain,
		Fid:       inf.Id,
		Elapse:    time.Since(startTime).Nanoseconds(),
		Type:      metadata.SucCreate.String(), // compatible with 1.0
		EventId:   "",                          // compatible with 1.0
		ThreadId:  "",                          // compatible with 1.0
		Description: fmt.Sprintf("%s[Copy], client: %s, srcFid: %s, dst: %s", metadata.SucCreate.String(),
			peerAddr, req.SrcFid, (*handler).Name()),
	}
	err = s.eventOp.SaveEvent(event)
	if err != nil {
		// log into file instead return.
		glog.Warningf("%s, error: %v", event.String(), err)
	}

	rep = &transfer.CopyRep{
		Fid: inf.Id,
	}
	mf = func() (interface{}, string) {
		return rep, fmt.Sprintf("copy new fid %s, srcfid %s, srddomain %d, dstdomain %d", inf.Id, req.SrcFid, req.SrcDomain, req.DstDomain)
	}

	return mf, nil
}
