package server

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/notice"
)

var (
	HealthCheckInterval = flag.Int("health-check-interval", 30, "health check interval in seconds.")
	HealthCheckTimeout  = flag.Int("health-check-timeout", 5, "health check timeout in seconds.")
	HealthCheckManually = flag.Bool("health-check-manually", true, "true for checking health manually.")

	recoveryBufferSize = flag.Int("recovery-buffer-size", 100000, "size of channel for each DFSFileHandler.")
	recoveryInterval   = flag.Int("recovery-interval", 600, "interval in seconds for recovery event inspection.")
	recoveryBatchSize  = flag.Int("recovery-batch-size", 100000, "batch size for recovery event inspection.")

	segmentDeletion = flag.Bool("segment-deletion", false, "true for remove segment.")

	daysKeepInCache = flag.Int("days-keep-in-cache", 90, "days for file to keep in cache.")
)

// FileRecoveryInfo represents the information for file recovery.
type FileRecoveryInfo struct {
	Id     bson.ObjectId
	Fid    string
	Domain int64
}

func (rInfo *FileRecoveryInfo) String() string {
	return fmt.Sprintf("fid: %s, domain: %d, id: %s",
		rInfo.Fid, rInfo.Domain, rInfo.Id.Hex())
}

// HandlerSelector selects a perfect file handler for dfs server.
type HandlerSelector struct {
	segments    []*metadata.Segment
	segmentLock sync.RWMutex

	recoveries   map[string]chan *FileRecoveryInfo
	recoveryLock sync.RWMutex

	shardHandlers map[string]*ShardHandler
	handlerLock   sync.RWMutex

	degradeShardHandler *ShardHandler

	dfsServer *DFSServer

	backStoreShard *metadata.Shard
	minorHandler   fileop.DFSFileMinorHandler
}

func (hs *HandlerSelector) addRecovery(name string, rInfo chan *FileRecoveryInfo) {
	hs.recoveryLock.Lock()
	defer hs.recoveryLock.Unlock()

	hs.recoveries[name] = rInfo
}

func (hs *HandlerSelector) delRecovery(name string) {
	hs.recoveryLock.Lock()
	defer hs.recoveryLock.Unlock()

	delete(hs.recoveries, name)
}

func (hs *HandlerSelector) getRecovery(name string) (chan *FileRecoveryInfo, bool) {
	hs.recoveryLock.RLock()
	defer hs.recoveryLock.RUnlock()

	rInfo, ok := hs.recoveries[name]

	return rInfo, ok
}

func (hs *HandlerSelector) setShardHandler(handlerName string, handler *ShardHandler) {
	hs.handlerLock.Lock()
	defer hs.handlerLock.Unlock()

	hs.shardHandlers[handlerName] = handler
}

func (hs *HandlerSelector) delShardHandler(handlerName string) {
	hs.handlerLock.Lock()
	defer hs.handlerLock.Unlock()

	delete(hs.shardHandlers, handlerName)
}

func (hs *HandlerSelector) getShardHandler(handlerName string) (*ShardHandler, bool) {
	hs.handlerLock.RLock()
	defer hs.handlerLock.RUnlock()

	h, ok := hs.shardHandlers[handlerName]
	return h, ok
}

// updateHandler creates or updates a handler for given shard.
// op 1 for add a handler, 2 for delete a handler.
func (hs *HandlerSelector) updateHandler(shard *metadata.Shard, op int) {
	switch op {
	case 1:
		for {
			err := hs.addHandler(shard)
			if err == nil {
				break
			}
			t := time.Duration(15+rand.Intn(10)) * time.Second
			glog.Infof("Retry to start handler '%s' after %v.", shard.Name, t)
			time.Sleep(t)
		}
	case 2:
		hs.deleteHandler(shard.Name)
	}
}

func (hs *HandlerSelector) deleteHandler(handlerName string) {
	if sh, ok := hs.getShardHandler(handlerName); ok {
		if err := sh.handler.Close(); err != nil {
			glog.Warningf("Failed to close the old handler: %v", sh.handler.Name())
		}

		sh.Shutdown()
		hs.delShardHandler(handlerName)

		glog.Infof("Succeeded to delete handler, shard: %s", handlerName)
	}
}

func inferShardType(shard *metadata.Shard) {
	if shard.ShdType != metadata.RegularServer {
		return
	}

	if len(shard.VolHost) != 0 && len(shard.VolName) != 0 { // GlusterFS
		shard.ShdType = metadata.Glustergo
	} else { // GridFS
		shard.ShdType = metadata.Gridgo
	}
}

func (hs *HandlerSelector) addHandler(shard *metadata.Shard) (err error) {
	var handler fileop.DFSFileHandler

	inferShardType(shard)

	switch shard.ShdType {
	case metadata.Glustra:
		handler, err = fileop.NewGlustraHandler(shard, filepath.Join(*logDir, shard.Name))
	case metadata.Seadra:
		handler, err = fileop.NewSeadraHandler(shard)
	case metadata.Glustergo:
		handler, err = fileop.NewGlusterHandler(shard, filepath.Join(*logDir, shard.Name))
	case metadata.Gridgo:
		handler, err = fileop.NewGridFsHandler(shard)
	case metadata.DegradeServer:
		handler, err = fileop.NewGridFsHandler(shard)
	case metadata.BackstoreServer:
		//skip
	default:
		err = fmt.Errorf("invalid shard type")
	}
	if err != nil || handler == nil {
		glog.Warningf("Failed to create handler, shard: %s, type: %d, error: %v.", shard.Name, shard.ShdType, err)
		return
	}

	if shard.ShdType == metadata.DegradeServer {
		if hs.degradeShardHandler != nil {
			glog.Warningf("Failed to create degrade server, since we already have one, shard: %v.", shard)
			return
		}

		dh := fileop.NewDegradeHandler(handler, hs.dfsServer.reOp)
		hs.degradeShardHandler = NewShardHandler(dh, statusOk, hs)
		glog.Infof("Succeeded to create degrade handler '%s'.", shard.Name)
		return
	}

	if hs.minorHandler != nil {
		if shard.ShdType == metadata.Glustergo {
			if err := hs.minorHandler.InitVolumeCB(shard.VolHost, shard.VolName, shard.VolBase); err != nil {
				glog.Info("Failed to initialize volume for handler %s, %v", hs.minorHandler.Name(), err)
				return err
			}

			handler = fileop.NewTeeHandler(handler, hs.minorHandler)
			glog.Infof("Succeeded to attach handler '%s' with minor '%s'.", handler.Name(), hs.minorHandler.Name())
		}
	}

	if hs.backStoreShard != nil {
		handler = fileop.NewBackStoreHandler(handler, hs.backStoreShard, hs.dfsServer.cacheOp)
		glog.Infof("Succeeded to attach handler '%s' with bs '%s'.", handler.Name(), hs.backStoreShard.Name)
	}

	if sh, ok := hs.getShardHandler(handler.Name()); ok {
		if err := sh.Shutdown(); err != nil {
			glog.Warningf("Failed to shutdown old handler, shard: '%s'.", sh.handler.Name())
		}
		glog.Infof("Succeded to shutdown old handler, shard: '%s'.", shard.Name)
	}

	sh := NewShardHandler(handler, statusOk, hs)

	hs.addRecovery(handler.Name(), sh.recoveryChan)

	glog.Infof("Succeeded to create handler '%s' type %d.", shard.Name, shard.ShdType)

	return
}

// getDfsFileHandler returns perfect file handlers to process file.
// The first returned handler is for normal handler,
// and the second one is for file migrating.
func (hs *HandlerSelector) getDFSFileHandler(domain int64) (*fileop.DFSFileHandler, *fileop.DFSFileHandler, error) {
	if len(hs.shardHandlers) == 0 {
		return nil, nil, fmt.Errorf("no handler")
	}

	seg := hs.FindPerfectSegment(domain)
	if seg == nil {
		return nil, nil, fmt.Errorf("no perfect server, domain %d", domain)
	}

	n, ok := hs.getShardHandler(seg.NormalServer)
	if !ok {
		return nil, nil, fmt.Errorf("no normal site '%s'", seg.NormalServer)
	}

	m, ok := hs.getShardHandler(seg.MigrateServer)
	if ok {
		return &(n.handler), &(m.handler), nil
	}

	return &(n.handler), nil, nil
}

// checkOrDegrade checks status of given handler,
// if status is offline, degrade.
func (hs *HandlerSelector) checkOrDegrade(handler *fileop.DFSFileHandler) (*fileop.DFSFileHandler, error) {
	if handler == nil { // Check for nil.
		return nil, fmt.Errorf("handler is nil")
	}

	if status, ok := hs.getHandlerStatus(*handler); ok && status == statusOk {
		return handler, nil
	}

	dh := hs.degradeShardHandler.handler
	if hs.degradeShardHandler.status == statusOk {
		glog.Errorf("!!! server %s degrade to %s", (*handler).Name(), dh.Name())
		return &dh, nil
	}

	return nil, fmt.Errorf("'%s' and '%s' not reachable", (*handler).Name(), dh.Name())
}

// getDfsFileHandlerForWrite returns perfect file handlers to write file.
func (hs *HandlerSelector) getDFSFileHandlerForWrite(domain int64) (*fileop.DFSFileHandler, error) {
	nh, mh, err := hs.getDFSFileHandler(domain)
	if err != nil {
		return nil, err
	}

	handler := nh
	if mh != nil {
		handler = mh
	}

	return hs.checkOrDegrade(handler)
}

// getDfsFileHandlerForRead returns perfect file handlers to read file.
func (hs *HandlerSelector) getDFSFileHandlerForRead(domain int64) (*fileop.DFSFileHandler, *fileop.DFSFileHandler, error) {
	n, m, err := hs.getDFSFileHandler(domain)
	if err != nil {
		return nil, nil, err
	}

	m, _ = hs.checkOrDegrade(m) // Need not check this error.
	n, err = hs.checkOrDegrade(n)
	// Need not return err, since we will verify the pair of n and m
	// outside this function.
	if err != nil {
		glog.Warningf("Ignored error: %v", err)
	}

	return n, m, nil
}

func (hs *HandlerSelector) updateHandlerStatus(h fileop.DFSFileHandler, status handlerStatus) {
	hs.handlerLock.Lock()
	defer hs.handlerLock.Unlock()

	hs.shardHandlers[h.Name()].updateStatus(status)
}

func (hs *HandlerSelector) getHandlerStatus(h fileop.DFSFileHandler) (handlerStatus, bool) {
	if *HealthCheckManually {
		// TODO(hanyh): update status from db.
		return statusOk, true
	}

	hs.handlerLock.RLock()
	defer hs.handlerLock.RUnlock()

	sh, ok := hs.shardHandlers[h.Name()]
	return sh.status, ok
}

// startShardNoticeRoutine starts a routine to receive and process notice
// from shard server and segment change.
func (hs *HandlerSelector) startShardNoticeRoutine() {
	s := hs.dfsServer
	go func() {
		data, errs := s.notice.CheckDataChange(notice.ShardServerPath)
		glog.Infof("A routine is ready to update shard.")

		for {
			select {
			case v := <-data:
				serverName := string(v)
				shard, err := s.mOp.LookupShardByName(serverName)
				if err != nil {
					if err == mgo.ErrNotFound {
						shard := &metadata.Shard{
							Name: serverName,
						}
						go hs.updateHandler(shard, 2 /* delete handler */)
						break
					}
					glog.Warningf("Failed to lookup shard %s, error: %v", serverName, err)
					break
				}

				go hs.updateHandler(shard, 1 /* add handler */)
			case err := <-errs:
				glog.Warningf("Failed to process shard notice, error: %v", err)
			}
		}
	}()

	go func() {
		data, errs := s.notice.CheckDataChange(notice.ShardChunkPath)
		glog.Infof("A routine is ready to update segment.")

		for {
			select {
			case v := <-data:
				segmentName := string(v)
				domain, err := strconv.Atoi(segmentName)
				if err != nil {
					break
				}

				if domain == -1 {
					hs.backfillSegment()
					break
				}

				seg, err := s.mOp.LookupSegmentByDomain(int64(domain))
				if err != nil {
					glog.Warningf("Failed to lookup segment %d, error: %v", domain, err)
					break
				}

				// Update segment in selector.
				hs.updateSegment(seg)
			case err := <-errs:
				glog.Warningf("Failed to process segment notice, error: %v", err)
			}
		}
	}()
}

// startRecoveryRoutine starts a recovery routine for every handler.
func (hs *HandlerSelector) startRecoveryDispatchRoutine() {
	go func() {
		ticker := time.NewTicker(time.Duration(*recoveryInterval) * time.Second)
		defer ticker.Stop()
		glog.Infof("A routine is ready for recovery dispatch.")

		for {
			select {
			case <-ticker.C:
				if err := hs.dispatchRecoveryEvent(*recoveryBatchSize, int64(*recoveryInterval)); err != nil {
					glog.Warningf("Recovery dispatch error, %v", err)
				}
			}
		}
	}()
}

// startBSCompactRoutine() starts a routine to compact backstore at midnight.
func (hs *HandlerSelector) startBSCompactRoutine() {
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()

		if hs.backStoreShard == nil || hs.backStoreShard.MasterUri == "" {
			return
		}
		master := strings.Split(hs.backStoreShard.MasterUri, ",")
		if len(master) == 0 {
			return
		}

		glog.Infof("Start a routine to compact backstore, %v", master)
		for {
			select {
			case <-ticker.C:
				h := time.Now().Hour()
				if h >= 1 && h < 2 {
					for _, uri := range master {
						resp, err := http.Get("http://" + uri + "/vol/vacuum")
						if err == nil {
							body, err := ioutil.ReadAll(resp.Body)
							if err == nil {
								glog.Infof("Succeeded to compact backstore, %s\n%s", uri, string(body))
							}

							resp.Body.Close()
							break
						}

						glog.Warningf("Failed to compact backstore, %s, %v", uri, err)
					}
				}
			}
		}

	}()
}

// startCachedFileRecoveryRoutine() starts a recovery routine for cached file.
func (hs *HandlerSelector) startCachedFileRecoveryRoutine() {
	if hs.backStoreShard == nil {
		glog.Warningln("No need to start cached file recovery routine without backstore.")
		return
	}

	cacheOp := hs.dfsServer.cacheOp

	go func() {
		ticker := time.NewTicker(time.Duration(*recoveryInterval) * time.Second)
		defer ticker.Stop()
		glog.Infof("Succeeded to start cached file recovery routine, triggered every %d seconds.", *recoveryInterval)

		for {
			select {
			case <-ticker.C:

				func() {
					defer func() {
						if r := recover(); r != nil {
							glog.Warningf("%v\n%s", r, getStack())
						}
					}()

					iter, err := cacheOp.GetCacheLogs(10 /* limit */)
					if err != nil {
						glog.Warningf("Failed to fetch cached log %v", err)
						return
					}
					defer iter.Close()

					cachelog := metadata.CacheLog{}
					for iter.Next(&cachelog) {
						handler, err := hs.getDFSFileHandlerForWrite(cachelog.Domain)
						if err != nil {
							glog.Warningf("Failed to get file handler for %s", cachelog.String())
							continue
						}

						backstoreHandler, ok := interface{}(*handler).(*fileop.BackStoreHandler)
						if !ok {
							glog.Warningf("Not a BackStoreHandler%T, %s", *handler, cachelog.String())
							continue
						}
						glusterHandler, ok := interface{}((*backstoreHandler).DFSFileHandler).(*fileop.GlusterHandler)
						if !ok {
							teeHandler, ok := interface{}((*backstoreHandler).DFSFileHandler).(*fileop.TeeHandler)
							if !ok {
								glog.Warningf("Not a TeeHandler %T, %s", (*backstoreHandler).DFSFileHandler, cachelog.String())
								continue
							}
							glusterHandler, ok = interface{}((*teeHandler).GetMajor()).(*fileop.GlusterHandler)
							if !ok {
								glog.Warningf("Not a GlusterHandler %T, %s", (*teeHandler).GetMajor(), cachelog.String())
								continue
							}
						}

						copyCachedFile(glusterHandler, hs.backStoreShard.MasterUri, cacheOp, &cachelog)
					}
				}()

				glog.V(5).Infof("Finished a recovery round.")
			}
		}
	}()

	// Remove cached log some days ago.
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				h := time.Now().Hour()
				if h >= 1 && h < 2 {
					glog.Infof("It's time to remove finished cache logs %d days ago.", *daysKeepInCache)
					cacheOp.RemoveFinishedCacheLogByTime(time.Now().AddDate(0, 0, -*daysKeepInCache).Unix())
				}
			}
		}

	}()
}

// dispachRecoveryEvent dispatches recovery events.
func (hs *HandlerSelector) dispatchRecoveryEvent(batchSize int, timeout int64) error {
	events, err := hs.dfsServer.reOp.GetEventsInBatch(batchSize, timeout)
	if err != nil {
		return err
	}

	for _, e := range events {
		h, err := hs.getDFSFileHandlerForWrite(e.Domain)
		if err != nil {
			glog.Warningf("Failed to get file handler for %d", e.Domain)
			continue
		}

		rec, ok := hs.getRecovery((*h).Name())
		if !ok {
			glog.Warningf("Failed to dispatch recovery event %s", e.String())
			continue
		}

		rec <- &FileRecoveryInfo{
			Id:     e.Id,
			Fid:    e.Fid,
			Domain: e.Domain,
		}
	}

	return nil
}

// updateSegments updates a segment.
func (hs *HandlerSelector) updateSegment(segment *metadata.Segment) {
	hs.segmentLock.Lock()
	defer hs.segmentLock.Unlock()

	hs.segments = updateSegment(hs.segments, segment)
}

func (hs *HandlerSelector) backfillSegment() {
	hs.segmentLock.Lock()
	defer hs.segmentLock.Unlock()

	hs.segments = hs.dfsServer.mOp.FindAllSegmentsOrderByDomain()
}

func (hs *HandlerSelector) backfillShard() {
	// Initialize storage servers
	shards := hs.dfsServer.mOp.FindAllShards()

	// Check storage servers to ensure that there's a shard at least.
	if len(shards) == 0 && len(hs.segments) == 0 {
		shards = append(shards, &metadata.Shard{
			Age:     1, // seq no
			Name:    shardAddr.ShardDbName,
			Uri:     shardAddr.ShardDbUri,
			ShdType: metadata.Gridgo,
		})

		hs.segments = append(hs.segments, &metadata.Segment{
			Domain:        1, // from 1 to infinity.
			MigrateServer: "",
			NormalServer:  shardAddr.ShardDbName,
		})

		glog.Infof("Succeeded to create a minimal shard '%s'.", shardAddr.ShardDbName)
	} else {
		glog.Infof("Succeeded to get %d shards.", len(shards))
	}

	for _, shard := range shards {
		if shard.ShdType > metadata.MinorServer {
			h, err := hs.createMinorHandler(shard)
			if err != nil {
				glog.Warningf("Failed to create minor handler %s, %v.", shard.Name, err)
				continue
			}

			hs.minorHandler = h
			glog.Infof("Succeeded to create minor handler %s, type %d.", h.Name(), shard.ShdType)

			break
		}
	}

	for _, shard := range shards {
		if shard.ShdType == metadata.BackstoreServer {
			hs.backStoreShard = shard
			glog.Infof("Succeeded to create back store handler %s.", shard.Name)

			break
		}
	}

	var wg sync.WaitGroup
	for _, shard := range shards {
		if shard.ShdType == metadata.BackstoreServer || shard.ShdType > metadata.MinorServer {
			continue
		}

		wg.Add(1)
		go func(shd *metadata.Shard) {
			hs.updateHandler(shd, 1 /* add handler */)
			wg.Done()
		}(shard)
	}

	handlerOver := make(chan struct{})
	go func() {
		wg.Wait()
		close(handlerOver)
	}()

	tm := 20 * time.Second
	glog.Infof("Initializing handlers for %v.", tm)
	ticker := time.Tick(tm)
	select {
	case <-handlerOver:
		glog.Infof("All handlers initialized ok!")
	case <-ticker:
		glog.Infof("Handlers initialize timeout.........")
	}
}

func (hs *HandlerSelector) createMinorHandler(shard *metadata.Shard) (handler fileop.DFSFileMinorHandler, err error) {
	shd := *shard
	shd.ShdType -= metadata.MinorServer

	switch shd.ShdType {
	case metadata.Glusti:
		handler, err = fileop.NewGlustiHandler(&shd, filepath.Join(*logDir, shd.Name))
	case metadata.Glustra:
		handler, err = fileop.NewGlustraHandler(&shd, filepath.Join(*logDir, shd.Name))
	case metadata.Seadra:
		var h *fileop.SeadraHandler
		h, err = fileop.NewSeadraHandler(&shd)
		if err != nil {
			return
		}
		h.StartHealthCheckRoutine(time.Duration(*HealthCheckInterval)*time.Second, time.Duration(*HealthCheckTimeout)*time.Second)
		handler = h
	default:
		err = fmt.Errorf("invalid shard type")
	}
	return
}

func (hs *HandlerSelector) showSegments() {
	glog.Infof("Succeeded to get %d segments:", len(hs.segments))
	for _, seg := range hs.segments {
		glog.Infof("\tSegment: [Domain:%d, ns:%s, ms:%s]",
			seg.Domain, seg.NormalServer, seg.MigrateServer)
	}
}

// FindPerfectSegment finds a perfect segment for domain.
// Segments must be in ascending order.
func (hs *HandlerSelector) FindPerfectSegment(domain int64) *metadata.Segment {
	hs.segmentLock.RLock()
	defer hs.segmentLock.RUnlock()

	_, result := findPerfectSegment(hs.segments, domain)
	return result
}

func NewHandlerSelector(dfsServer *DFSServer) (*HandlerSelector, error) {
	hs := new(HandlerSelector)
	hs.dfsServer = dfsServer

	hs.recoveries = make(map[string]chan *FileRecoveryInfo)
	hs.shardHandlers = make(map[string]*ShardHandler)

	// Fill segment data.
	hs.backfillSegment()

	// Fill shard data.
	hs.backfillShard()

	hs.showSegments()

	return hs, nil
}

func updateSegment(segments []*metadata.Segment, segment *metadata.Segment) []*metadata.Segment {
	pos, result := findPerfectSegment(segments, segment.Domain)

	if result.Domain != segment.Domain { // Not found
		// Insert
		pos++
		rear := append([]*metadata.Segment{}, segments[pos:]...)
		segments = append(segments[:pos], segment)
		segments = append(segments, rear...)

		glog.Infof("Succeeded to insert segment [d:%d,n:%s,m:%s] at pos %d.", segment.Domain, segment.NormalServer, segment.MigrateServer, pos)
		return segments
	}

	// Found. Equal, remove it according to flag.
	if *segmentDeletion &&
		result.NormalServer == segment.NormalServer &&
		result.MigrateServer == segment.MigrateServer {
		segs := append(segments[:pos], segments[pos+1:]...)
		segments = segs
		glog.Infof("Succeeded to remove segment [d:%d,n:%s,m:%s] at pos %d.", segment.Domain, segment.NormalServer, segment.MigrateServer, pos)
		return segments
	}

	// Not equal, update it.
	segments[pos] = segment
	glog.Infof("Succeeded to update segment [d:%d,n:%s,m:%s] at pos %d.", segment.Domain, segment.NormalServer, segment.MigrateServer, pos)
	return segments
}

func findPerfectSegment(segments []*metadata.Segment, domain int64) (int, *metadata.Segment) {
	var pos int
	var result *metadata.Segment

	for i, seg := range segments {
		if domain > seg.Domain {
			pos, result = i, seg
			continue
		} else if domain == seg.Domain {
			pos, result = i, seg
			break
		} else {
			break
		}
	}

	return pos, result
}

// healthCheck detects a handler for its health.
// If detection times out, return false.
func healthCheck(handler fileop.DFSFileHandler) handlerStatus {
	healthStatus := make(chan int, 1)
	ticker := time.NewTicker(time.Duration(*HealthCheckTimeout) * time.Second)
	defer func() {
		ticker.Stop()
	}()

	go func() {
		healthStatus <- handler.HealthStatus()
		close(healthStatus)
	}()

	select {
	case status := <-healthStatus:
		if status != fileop.HealthOk {
			glog.Warningf("check handler %v %d", handler.Name(), status)
		}
		instrument.HealthCheckStatus <- &instrument.Measurements{
			Name:  handler.Name(),
			Biz:   fmt.Sprintf("%d", status),
			Value: 1.0,
		}
		return NewHandlerStatus(status == fileop.HealthOk)
	case <-ticker.C:
		glog.Warningf("check handler %v expired", handler.Name())
		instrument.HealthCheckStatus <- &instrument.Measurements{
			Name:  handler.Name(),
			Biz:   "expired",
			Value: 1.0,
		}
		return statusFailure
	}
}

func copyCachedFile(glusterHandler *fileop.GlusterHandler, masterUri string, cacheOp *metadata.CacheLogOp, cachelog *metadata.CacheLog) (state int) {
	defer func() {
		switch state {
		case metadata.CACHELOG_STATE_FINISHED:
			sum := (cachelog.RetryTimes - 1) * cachelog.RetryTimes / 2
			instrument.CachedFileRetryTimesGauge.Sub(float64(sum))
			if instrument.GetRetryTimesFromGauge() <= 0 {
				instrument.CachedFileRetryTimesGauge.Set(0.0)
				glog.V(5).Infoln("Reset retry times gauge.")
			}
			instrument.CachedFileRetryTimes.Observe(float64(cachelog.RetryTimes - 1))
			instrument.CachedFileCount.WithLabelValues(instrument.CACHED_FILE_RECOVER_SUC).Inc()
			glog.Infof("Succeeded to recover cached file %s.", cachelog)
		case metadata.CACHELOG_STATE_PENDING:
			instrument.CachedFileRetryTimesGauge.Add(float64(cachelog.RetryTimes))
			fallthrough
		default:
			instrument.CachedFileCount.WithLabelValues(instrument.CACHED_FILE_RECOVER_FAILED).Inc()
			glog.Warningf("Failed to recover cached file %d, %s.", state, cachelog)
		}
	}()

	// for debug
	if glog.V(100) {
		cacheOp.SaveOrUpdate(cachelog)
		return metadata.CACHELOG_STATE_PENDING
	}

	gFile, err := glusterHandler.CreateGlusterFile(cachelog.Domain, cachelog.Fid)
	if err != nil {
		glog.Warningf("Failed to create gluster file while recovering, %v.", err)
		cacheOp.SaveOrUpdate(cachelog)
		return metadata.CACHELOG_STATE_PENDING
	}
	defer gFile.Close()

	cachedFile, err := fileop.OpenCachedFile(cachelog.CacheId, cachelog.Domain, masterUri, cachelog.CacheChunkSize)
	if err != nil {
		glog.Warningf("Failed to open cache file while recovering, %v.", err)
		cachelog.State = metadata.CACHELOG_STATE_SRC_DAMAGED
		cacheOp.SaveOrUpdate(cachelog)
		return metadata.CACHELOG_STATE_SRC_DAMAGED
	}
	defer cachedFile.Close()

	// copy content from cached file to glusterfs file.
	buf := make([]byte, 4096)
	for {
		n, err := cachedFile.Read(buf)
		if n > 0 {
			_, err := gFile.Write(buf[:n])
			if err != nil {
				cacheOp.SaveOrUpdate(cachelog)
				glog.Warningf("Failed to copy while recovering, %v.", err)
				return metadata.CACHELOG_STATE_PENDING
			}
		}
		if err == io.EOF {
			cachelog.State = metadata.CACHELOG_STATE_FINISHED
			cacheOp.SaveOrUpdate(cachelog)
			return metadata.CACHELOG_STATE_FINISHED
		}
		if err != nil {
			glog.Warningf("Failed to copy while recovering, %v.", err)
			cachelog.State = metadata.CACHELOG_STATE_SRC_DAMAGED
			cacheOp.SaveOrUpdate(cachelog)
			return metadata.CACHELOG_STATE_SRC_DAMAGED
		}
	}
}
