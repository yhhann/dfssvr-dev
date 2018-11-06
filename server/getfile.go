package server

import (
	"fmt"
	"io"
	"time"

	"github.com/golang/glog"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// GetFile gets a file from server.
func (s *DFSServer) GetFile(req *transfer.GetFileReq, stream transfer.FileTransfer_GetFileServer) (err error) {
	serviceName := "GetFile"
	peerAddr := getPeerAddressString(stream.Context())
	glog.V(3).Infof("%s start, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Id) == 0 || req.Domain <= 0 {
		return fmt.Errorf("invalid request [%v]", req)
	}

	return streamFunc(s.getFileStream).withStreamDeadline(serviceName, req, stream, serviceName, peerAddr, s)
}

func (s *DFSServer) getFileStream(request interface{}, grpcStream interface{}, args []interface{}) (msgFunc, error) {
	var mf msgFunc
	startTime := time.Now()

	serviceName, peerAddr, err := extractStreamFuncParams(args)
	if err != nil {
		return mf, err
	}

	req, stream, err := verifyFileStream(request, grpcStream)
	if err != nil {
		return mf, err
	}

	mf = func() (interface{}, string) {
		return nil, fmt.Sprintf("getfile, fid %s, domain %d", req.Id, req.Domain)
	}

	_, file, err := s.openFileForRead(req.Id, req.Domain)
	if err != nil {
		if err == meta.FileNotFound {
			event := &metadata.Event{
				EType:       metadata.FailRead,
				Timestamp:   util.GetTimeInMilliSecond(),
				Domain:      req.Domain,
				Fid:         req.Id,
				Description: fmt.Sprintf("%s, client %s", metadata.FailRead.String(), peerAddr),
			}
			if er := s.eventOp.SaveEvent(event); er != nil {
				// log into file instead return.
				glog.Warningf("%s, error: %v", event.String(), er)
			}
		}
		return mf, err
	}
	defer file.Close()

	fi := file.GetFileInfo()
	fi.Domain = req.Domain
	mf = func() (interface{}, string) {
		return nil, fmt.Sprintf("getfile, fid %s, domain %d, size %d, biz %s, name %s", fi.Id, fi.Domain, fi.Size, fi.Biz, fi.Name)
	}

	// check timeout, for test.
	if *enablePreJudge {
		if dl, ok := getDeadline(stream); ok {
			given := dl.Sub(startTime)
			expected, err := checkTimeout(fi.Size, rRate, given)
			if err != nil {
				instrument.PrejudgeExceed <- &instrument.Measurements{
					Name:  serviceName,
					Value: float64(expected.Nanoseconds()),
				}
				glog.Warningf("%s, timeout return early, expected %v, given %v", serviceName, expected, given)
				return mf, err
			}
		}
	}

	// First, we send file info.
	err = stream.Send(&transfer.GetFileRep{
		Result: &transfer.GetFileRep_Info{
			Info: fi,
		},
	})
	if err != nil {
		return mf, err
	}

	// Second, we send file content in a loop.
	var off int64
	b := make([]byte, fileop.NegotiatedChunkSize)
	for {
		length, err := file.Read(b)
		if length > 0 {
			err = stream.Send(&transfer.GetFileRep{
				Result: &transfer.GetFileRep_Chunk{
					Chunk: &transfer.Chunk{
						Pos:     off,
						Length:  int64(length),
						Payload: b[:length],
					},
				},
			})
			if err != nil {
				_, desc := mf()
				glog.Warningf("GetFile, send to client error %v, %s", err, desc)
				return mf, err
			}
		}

		if err == io.EOF || (err == nil && length == 0) {
			nsecs := time.Since(startTime).Nanoseconds() + 1
			rate := off * 8 * 1e6 / nsecs // in kbit/s

			instrumentGetFile(off, rate, serviceName, fi.Biz)
			glog.V(3).Infof("GetFile ok, %s, length %d, elapse %d, rate %d kbit/s", req, off, nsecs, rate)

			return mf, nil
		}
		if err != nil {
			_, desc := mf()
			glog.Warningf("GetFile, read source error %v, %s", err, desc)
			return mf, err
		}

		off += int64(length)
	}
}

func verifyFileStream(request interface{}, grpcStream interface{}) (req *transfer.GetFileReq, stream transfer.FileTransfer_GetFileServer, err error) {
	req, ok := request.(*transfer.GetFileReq)
	if !ok {
		return nil, nil, AssertionError
	}
	stream, ok = grpcStream.(transfer.FileTransfer_GetFileServer)
	if !ok {
		return nil, nil, AssertionError
	}

	return req, stream, nil
}
