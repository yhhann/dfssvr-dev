package server

import (
	"flag"
	"fmt"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

var (
	logRemoveCommand = flag.Bool("log-remove-cmd", false, "log the remove command for audit.")
)

// Remove deletes a file.
func (s *DFSServer) RemoveFile(ctx context.Context, req *transfer.RemoveFileReq) (*transfer.RemoveFileRep, error) {
	serviceName := "RemoveFile"
	peerAddr := getPeerAddressString(ctx)
	glog.V(3).Infof("%s, client: %s, %v", serviceName, peerAddr, req)

	clientDesc := ""
	if req.GetDesc() != nil {
		clientDesc = req.GetDesc().Desc
	}

	if len(req.Id) == 0 || req.Domain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := bizFunc(s.removeBiz).withDeadline(serviceName, ctx, req, peerAddr, clientDesc)
	if err != nil {
		return nil, err
	}

	if result, ok := t.(*transfer.RemoveFileRep); ok {
		return result, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) removeBiz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("parameter number %d", len(args))
	}

	peerAddr, ok := args[0].(string)
	clientDesc, ok := args[1].(string)
	req, ok := r.(*transfer.RemoveFileReq)
	if !ok {
		return nil, AssertionError
	}

	startTime := time.Now()

	// log the remove command for audit.
	if *logRemoveCommand {
		event := &metadata.Event{
			EType:     metadata.CommandDelete,
			Timestamp: util.GetTimeInMilliSecond(),
			Domain:    req.Domain,
			Fid:       req.Id,
			Elapse:    -1,
			Description: fmt.Sprintf("%s, client %s\n%s", metadata.CommandDelete.String(),
				peerAddr, clientDesc),
		}
		if er := s.eventOp.SaveEvent(event); er != nil {
			// log into file instead return.
			glog.Warningf("%s, error: %v", event.String(), er)
		}
	}

	var mf msgFunc
	var rep *transfer.RemoveFileRep
	mf = func() (interface{}, string) {
		return rep, fmt.Sprintf("remove, fid %s, domain %d", req.Id, req.Domain)
	}

	var p fileop.DFSFileHandler
	var fm *meta.File

	nh, mh, err := s.selector.getDFSFileHandlerForRead(req.Domain)
	if err != nil {
		glog.Warningf("RemoveFile, failed to get handler for read, error: %v", err)
		return mf, err
	}

	var dr DeleteResult
	if nh != nil {
		p = *nh
		dr.nresult, dr.nMeta, dr.nerr = p.Remove(req.Id, req.Domain)
	}
	if mh != nil {
		p = *mh
		dr.mresult, dr.mMeta, dr.merr = p.Remove(req.Id, req.Domain)
	}

	result, fm, err := dr.result()

	// space log.
	if err == nil && result {
		slog := &metadata.SpaceLog{
			Fid:       fm.Id,
			Domain:    fm.Domain,
			Uid:       fm.UserId,
			Biz:       fm.Biz,
			Size:      fm.Size,
			Timestamp: time.Now(),
			Type:      metadata.DeleteType.String(),
		}
		if er := s.spaceOp.SaveSpaceLog(slog); er != nil {
			glog.Warningf("%s, error: %v", slog.String(), er)
		}
	}

	// log the remove result for audit.
	resultEvent := &metadata.Event{
		EType:     metadata.SucDelete,
		Timestamp: util.GetTimeInMilliSecond(),
		Domain:    req.Domain,
		Fid:       req.Id,
		Elapse:    time.Since(startTime).Nanoseconds(),
		Description: fmt.Sprintf("%s, client %s, result %t, from %v, err %v",
			metadata.SucDelete.String(), peerAddr, result, p.Name(), err),
	}
	if er := s.eventOp.SaveEvent(resultEvent); er != nil {
		// log into file instead return.
		glog.Warningf("Failed to save remove event, %v", er)
	}

	if result {
		glog.V(3).Infof("RemoveFile, succeeded to remove entity %s from %v, err %v.", req.Id, p.Name(), err)
	} else {
		glog.V(3).Infof("RemoveFile, succeeded to remove reference %s from %v, err %v.", req.Id, p.Name(), err)
	}

	rep = &transfer.RemoveFileRep{
		Result: result,
	}
	mf = func() (interface{}, string) {
		return rep, fmt.Sprintf("remove entity %t, fid %s, domain %d, from %v", rep.Result, req.Id, req.Domain, p.Name())
	}

	return mf, nil
}

type DeleteResult struct {
	nerr    error
	merr    error
	nresult bool
	mresult bool
	nMeta   *meta.File
	mMeta   *meta.File
}

func (dr *DeleteResult) result() (bool, *meta.File, error) {
	if dr.nerr != nil && dr.merr != nil {
		return false, nil, fmt.Errorf("%v,%v", dr.nerr, dr.merr)
	}

	fm := dr.nMeta
	if fm == nil && dr.mMeta != nil {
		fm = dr.mMeta
	}

	r := dr.nresult || dr.mresult

	if fm == nil && r {
		return false, nil, fmt.Errorf("file meta is nil.")
	}

	return r, fm, nil
}
