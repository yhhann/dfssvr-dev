package fileop

import (
	"flag"
	"io"
	"strings"
	"time"

	"github.com/golang/glog"

	"jingoal.com/dfs/conf"
	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/seaweedfs-adaptor/weedfs"
)

var (
	bsWithRealName = flag.Bool("bs-with-real-name", false, "create backstore file with the real name.")
	cacheDuration  = flag.Duration("cache-duration", 3*time.Hour, "duration for cache file arbitrarily")

	NegotiatedChunkSize = int64(1048576)
)

// BackStoreHandler implements interface DFSFileHandler.
type BackStoreHandler struct {
	// embedded DFSFileHandler
	DFSFileHandler

	// shard for back store
	BackStoreShard *metadata.Shard

	// cahce log operator
	logOp *metadata.CacheLogOp
}

// Create creates a DFSFile for write
func (bsh *BackStoreHandler) Create(info *transfer.FileInfo) (DFSFile, error) {
	originalFile, err := bsh.DFSFileHandler.Create(info)
	if reErr, ok := err.(RecoverableFileError); ok {
		if !(isCacheFile(info.Domain) ||
			(isWriteToBackStore(info.Domain) && time.Since(instrument.ProgramStartTime) < *cacheDuration)) {
			glog.V(2).Infof("not write to cache.")
			return nil, reErr.Orig
		}
		glog.V(2).Infof("write to cache.")

		wFile, er := bsh.createCacheFile(info, originalFile)
		if er != nil {
			glog.Warningf("Failed to create cache file %v, %v", info, reErr.Orig)
			return nil, er
		}

		// log event.
		createInfo := originalFile.GetFileInfo()
		cachelog := metadata.CacheLog{
			Timestamp:      time.Now().Unix(),
			Domain:         createInfo.Domain,
			Fid:            createInfo.Id,
			CacheId:        wFile.Fid,
			CacheChunkSize: wFile.GetChunkSize(),
			Shard:          bsh.Name(),
			Cause:          reErr.Orig.Error(),
		}
		_, er = bsh.logOp.SaveOrUpdate(&cachelog)
		if er != nil {
			glog.Warningf("Failed to save cache log %s, %v", cachelog, er)
			return nil, er
		}

		instrument.CachedFileCount.WithLabelValues(instrument.CACHED_FILE_CACHED_SUC).Inc()
		glog.Infof("Succeeded to create cache file %s", cachelog)
		return NewBackStoreFile(wFile, originalFile, createInfo), nil
	}
	if err != nil {
		return nil, err
	}

	var wFile *weedfs.WeedFile
	if isWriteToBackStore(info.Domain) {
		wFile, err = bsh.createCacheFile(info, originalFile)
		if err != nil {
			return NewBackStoreFile(nil, originalFile, info), nil
		}
	}

	return NewBackStoreFile(wFile, originalFile, info), nil
}

func (bsh *BackStoreHandler) createCacheFile(info *transfer.FileInfo, originalFile DFSFile) (*weedfs.WeedFile, error) {
	fn := ""
	if *bsWithRealName {
		fn = info.Name
	}
	wFile, err := weedfs.Create(fn, info.Domain, bsh.BackStoreShard.MasterUri, bsh.BackStoreShard.Replica, bsh.BackStoreShard.DataCenter, bsh.BackStoreShard.Rack, NegotiatedChunkSize)
	if err != nil {
		instrument.BackstoreFileCounter <- &instrument.Measurements{
			Name:  "create_failed",
			Value: 1.0,
		}
		glog.Warningf("Failed to create wfile %v", err)
		return nil, err
	}

	originalFile.updateFileMeta(map[string]interface{}{
		MetaKey_WeedFid:   wFile.Fid,
		MetaKey_Chunksize: wFile.GetChunkSize(),
	})
	instrument.BackstoreFileCounter <- &instrument.Measurements{
		Name:  "created",
		Value: 1.0,
	}
	glog.V(3).Infof("Succeeded to create backstore file: %s, %s", wFile.Fid, info.Name)

	return wFile, nil
}

// Open opens a DFSFile for read
func (bsh *BackStoreHandler) Open(id string, domain int64) (DFSFile, error) {
	_, meta, info, err := bsh.DFSFileHandler.Find(id)
	if err != nil {
		return nil, err
	}

	glog.V(5).Infof("Read from bs %t, meta %+v", isReadFromBackStore(domain), meta)

	var wFile *weedfs.WeedFile
	readFromOrig := true
	if isReadFromBackStore(domain) && meta != nil && len(meta.Fid) > 0 {
		readFromOrig = false
		wFile, err = weedfs.Open(meta.Fid, domain, bsh.BackStoreShard.MasterUri)
		if err != nil {
			instrument.BackstoreFileCounter <- &instrument.Measurements{
				Name:  "opene_failed",
				Value: 1.0,
			}
			glog.Warningf("Failed to open backstore file %v", err)
			readFromOrig = true
		} else {
			if meta.ChunkSize > 0 {
				wFile.SetChunkSize(meta.ChunkSize)
			}
			instrument.BackstoreFileCounter <- &instrument.Measurements{
				Name:  "opened",
				Value: 1.0,
			}
			glog.V(3).Infof("Succeeded to open backstore file: %s, %s", id, wFile.Fid)
		}
	}

	if readFromOrig {
		originalFile, err := bsh.DFSFileHandler.Open(id, domain)
		if err != nil {
			return nil, err
		}
		glog.V(3).Infof("Succeeded to open original file: %s", id)
		return NewBackStoreFile(nil, originalFile, info), nil
	}

	return NewBackStoreFile(wFile, nil, info), nil
}

// Remove deletes a file by its id.
func (bsh *BackStoreHandler) Remove(id string, domain int64) (bool, *meta.File, error) {
	_, meta, _, err := bsh.DFSFileHandler.Find(id)
	if err != nil {
		return true, nil, nil
	}

	result, fm, err := bsh.DFSFileHandler.Remove(id, domain)
	if err != nil {
		return result, fm, err
	}
	glog.V(3).Infof("Succeeded to remove file from %s %s, %t", bsh.DFSFileHandler.Name(), id, result)

	if result {
		if meta != nil && meta.Fid != "" {
			deleteResult, err := weedfs.Remove(meta.Fid, domain, bsh.BackStoreShard.MasterUri)
			if err != nil {
				instrument.BackstoreFileCounter <- &instrument.Measurements{
					Name:  "remove_failed",
					Value: 1.0,
				}
				glog.V(3).Infof("Failed to remove file %s from %s, %v",
					meta.Fid, strings.Join([]string{bsh.Name(), bsh.BackStoreShard.Name}, "@"), err)
			} else {
				instrument.BackstoreFileCounter <- &instrument.Measurements{
					Name:  "removed",
					Value: 1.0,
				}
				glog.V(3).Infof("Succeeded to remove file %s from %s, %t",
					meta.Fid, strings.Join([]string{bsh.Name(), bsh.BackStoreShard.Name}, "@"), deleteResult)
			}
		}
	}

	return result, fm, err
}

// Close releases resources the handler holds.
func (bsh *BackStoreHandler) Close() error {
	return bsh.DFSFileHandler.Close()
}

// Duplicate duplicates an entry for a file.
func (bsh *BackStoreHandler) Duplicate(oid string, domain int64) (string, error) {
	return bsh.DFSFileHandler.Duplicate(oid, domain)
}

// Find finds a file, if the file not exists, return empty string.
// If the file exists, return its file id.
// If the file exists and is a duplication, return its primitive file id.
func (bsh *BackStoreHandler) Find(fid string) (string, *DFSFileMeta, *transfer.FileInfo, error) {
	return bsh.DFSFileHandler.Find(fid)
}

// Name returns handler's name.
func (bsh *BackStoreHandler) Name() string {
	return bsh.DFSFileHandler.Name()
}

// HealthStatus returns the status of node health.
func (bsh *BackStoreHandler) HealthStatus() int {
	return bsh.DFSFileHandler.HealthStatus()
}

// FindByMd5 finds a file by its md5.
func (bsh *BackStoreHandler) FindByMd5(md5 string, domain int64, size int64) (string, error) {
	return bsh.DFSFileHandler.FindByMd5(md5, domain, size)
}

func NewBackStoreHandler(originalHandler DFSFileHandler, bsShard *metadata.Shard, logOp *metadata.CacheLogOp) *BackStoreHandler {
	handler := BackStoreHandler{
		DFSFileHandler: originalHandler,
		BackStoreShard: bsShard,
		logOp:          logOp,
	}

	return &handler
}

// BackStoreFile implements DFSFile.
type BackStoreFile struct {
	// embedded DFSFile
	DFSFile

	info *transfer.FileInfo

	// back store file
	bs    *weedfs.WeedFile
	bsErr error
}

// GetFileInfo returns file info.
func (d *BackStoreFile) GetFileInfo() *transfer.FileInfo {
	if d.DFSFile != nil {
		return d.DFSFile.GetFileInfo()
	}

	return d.info
}

// Write writes a byte buffer into back store file.
func (d *BackStoreFile) Write(p []byte) (n int, err error) {
	if d.DFSFile != nil {
		n, err = d.DFSFile.Write(p)
	}

	if d.bs != nil && d.bsErr == nil {
		_, d.bsErr = d.bs.Write(p)
	}

	return
}

// Read reads a byte buffer from back store file.
func (d *BackStoreFile) Read(p []byte) (n int, err error) {
	if d.bs != nil {
		n, err = d.bs.Read(p)
		if glog.V(6) {
			glog.Infof("read from weed %d, %v", n, err)
		}
		return
	}

	if d.DFSFile != nil {
		return d.DFSFile.Read(p)
	}

	return 0, NoEntityError
}

// Close closes a back store file.
func (d *BackStoreFile) Close() (err error) {
	if d.bs != nil && d.bsErr == nil {
		err = d.bs.Close()
	}

	if d.DFSFile != nil {
		err = d.DFSFile.Close()
	}

	return
}

// hasEntity returns if the file has entity.
func (d *BackStoreFile) hasEntity() bool {
	return true
}

// NewBackStoreFile creates a new back store file.
func NewBackStoreFile(bs *weedfs.WeedFile, file DFSFile, info *transfer.FileInfo) *BackStoreFile {
	return &BackStoreFile{
		DFSFile: file,
		bs:      bs,
		info:    info,
	}
}

func OpenCachedFile(fid string, domain int64, masterUri string, chunkSize int64) (io.ReadCloser, error) {
	wf, err := weedfs.Open(fid, domain, masterUri)
	if err == nil && chunkSize > 0 {
		wf.SetChunkSize(chunkSize)
	}

	return wf, err
}

func isReadFromBackStore(domain int64) bool {
	ff, err := conf.GetFlag(conf.FlagKeyReadFromBackStore)
	if err != nil {
		glog.Warningf("feature %s error %v", conf.FlagKeyReadFromBackStore, err)
		return false
	}

	return ff.DomainHasAccess(uint32(domain))
}

func isWriteToBackStore(domain int64) bool {
	ff, err := conf.GetFlag(conf.FlagKeyBackStore)
	if err != nil {
		glog.Warningf("feature %s error %v", conf.FlagKeyBackStore, err)
		return false
	}

	return ff.DomainHasAccess(uint32(domain))
}

func isCacheFile(domain int64) bool {
	ff, err := conf.GetFlag(conf.FlagKeyCacheFile)
	if err != nil {
		glog.Warningf("feature %s error %v", conf.FlagKeyCacheFile, err)
		return false
	}

	return ff.DomainHasAccess(uint32(domain))
}
