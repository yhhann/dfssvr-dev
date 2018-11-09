package fileop

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"
	"time"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2/bson"

	dra "jingoal.com/dfs/cassandra"
	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/seaweedfs-adaptor/weedfs"
)

// SeadraHandler implements DFSFileMinorHandler.
type SeadraHandler struct {
	*metadata.Shard

	draOp  *dra.DraOpImpl
	duplfs meta.FileMetaOp
}

// Name returns handler's name.
func (h *SeadraHandler) Name() string {
	return h.Shard.Name
}

// Close releases resources.
func (h *SeadraHandler) Close() error {
	// Do nothing for seaweedfs.
	return nil
}

// Create creates a DFSFile for write.
func (h *SeadraHandler) Create(info *transfer.FileInfo) (DFSFile, error) {
	oid := bson.NewObjectId()
	if bson.IsObjectIdHex(info.Id) {
		oid = bson.ObjectIdHex(info.Id)
	}

	fn := "" // file name ignored

	wFile, err := weedfs.Create(fn, info.Domain, h.MasterUri, h.Replica, h.DataCenter, h.Rack, 0)
	if err != nil {
		return nil, err
	}

	mf := &meta.File{
		Id:        oid.Hex(),
		Biz:       info.Biz,
		Name:      info.Name,
		UserId:    fmt.Sprintf("%d", info.User),
		Domain:    info.Domain,
		ChunkSize: int(wFile.GetChunkSize()),
		Type:      meta.EntitySeaweedFS,
		ExtAttr: map[string]string{
			MetaKey_WeedFid: wFile.Fid,
		},
	}

	file := &SeadraFile{
		handler:  h,
		md5:      md5.New(),
		mode:     FileModeWrite,
		meta:     mf,
		WeedFile: wFile,
	}

	return file, nil
}

// Open opens a file for read.
func (h *SeadraHandler) Open(id string, domain int64) (DFSFile, error) {
	mf, err := h.duplfs.Find(id)
	if err != nil {
		return nil, err
	}

	wid := mf.ExtAttr[MetaKey_WeedFid]
	wFile, err := weedfs.Open(wid, mf.Domain, h.Shard.MasterUri)
	if err != nil {
		return nil, err
	}

	file := &SeadraFile{
		handler:  h,
		mode:     FileModeRead,
		meta:     mf,
		WeedFile: wFile,
	}

	return file, nil
}

// Duplicate duplicates an entry for a file.
func (h *SeadraHandler) Duplicate(fid string, domain int64) (string, error) {
	return h.duplfs.DuplicateWithId(fid, "", time.Time{})
}

// Find finds a file. If the file not exists, return empty string.
// If the file exists and is a duplication, return its primitive file ID.
// If the file exists, return its file ID.
func (h *SeadraHandler) Find(id string) (string, *DFSFileMeta, *transfer.FileInfo, error) {
	f, err := h.duplfs.Find(id)
	if err != nil {
		return "", nil, nil, err
	}

	glog.V(3).Infof("Succeeded to find file %s, return %s", id, f.Id)

	return f.Id, getFileMeta(f), transfer.File2Info(f), nil
}

// Remove deletes file by its id and domain.
func (h *SeadraHandler) Remove(id string, domain int64) (bool, *meta.File, error) {
	result, entityId, err := h.duplfs.Delete(id)
	if err != nil {
		glog.Warningf("Failed to remove file %s %d from %s, %s.", id, domain, h.Name(), err)
		return false, nil, err
	}

	var m *meta.File
	if result {
		m, err = h.duplfs.Find(entityId)
		if err != nil {
			return false, nil, err
		}
		h.draOp.RemoveFile(entityId)

		wid := m.ExtAttr[MetaKey_WeedFid]
		_, err := weedfs.Remove(wid, m.Domain, h.Shard.MasterUri)
		if err != nil {
			glog.Warningf("Failed to remove file %s %d from %s, %s.", wid, m.Domain, h.Name(), err)
			return false, nil, err
		}
	}

	return result, m, nil
}

// FindByMd5 finds a file by its md5.
func (h *SeadraHandler) FindByMd5(md5 string, domain int64, size int64) (string, error) {
	file, err := h.duplfs.FindByMd5(md5, domain) // ignore size
	if err != nil {
		return "", err
	}

	return file.Id, nil
}

// Create creates a DFSFile with the given id.
func (h *SeadraHandler) CreateWithGivenId(info *transfer.FileInfo) (DFSFile, error) {
	return h.Create(info)
}

// Duplicate duplicates an entry with the given id.
func (h *SeadraHandler) DuplicateWithGivenId(primaryId string, dupId string) (string, error) {
	return h.duplfs.DuplicateWithId(primaryId, dupId, time.Time{})
}

// HealthStatus returns the status of node health.
func (h *SeadraHandler) HealthStatus() int {
	// hide the real result of health check and just return ok.
	return HealthOk
}

func (h *SeadraHandler) health() int {
	// check cassandra
	if err := h.draOp.HealthCheck(transfer.ServerId); err != nil {
		return MetaNotHealthy
	}

	// seaweedfs health check
	if err := SeaweedFSHealthCheck(h.MasterUri, h.Replica, h.DataCenter, h.Rack, transfer.ServerId); err != nil {
		return StoreNotHealthy
	}

	return HealthOk
}

// healthCheck detects a handler for its health.
func (h *SeadraHandler) healthCheck(timeout time.Duration) {
	healthStatus := make(chan int, 1)
	ticker := time.NewTicker(timeout)
	defer func() {
		ticker.Stop()
	}()

	go func() {
		healthStatus <- h.health()
		close(healthStatus)
	}()

	select {
	case status := <-healthStatus:
		if status != HealthOk {
			glog.Warningf("check handler '%s' %s.", h.Name(), healthStatus2String(status))
		}
		instrument.HealthCheckStatus <- &instrument.Measurements{
			Name:  h.Name(),
			Biz:   fmt.Sprintf("%d", status),
			Value: 1.0,
		}
	case <-ticker.C:
		glog.Warningf("check handler '%s' expired.", h.Name())
		instrument.HealthCheckStatus <- &instrument.Measurements{
			Name:  h.Name(),
			Biz:   "expired",
			Value: 1.0,
		}
	}
}

func (h *SeadraHandler) StartHealthCheckRoutine(interval time.Duration, timeout time.Duration) {
	h.healthCheck(timeout)

	go func() {
		tick := time.Tick(interval)
		for {
			select {
			case <-tick:
				h.healthCheck(timeout)
			}
		}
	}()
}

// InitVolumeCB is a callback function invoked by major to initialize volume.
func (h *SeadraHandler) InitVolumeCB(host, name, base string) error {
	// leave it empty.
	return nil
}

// NewSeadraHandler creates a SeadraHandler.
func NewSeadraHandler(shd *metadata.Shard) (*SeadraHandler, error) {
	handler := &SeadraHandler{
		Shard: shd,
	}

	if shd.ShdType != metadata.Seadra {
		return nil, fmt.Errorf("invalid shard type %d.", shd.ShdType)
	}

	seeds := strings.Split(shd.Uri, ",")
	handler.draOp = dra.NewDraOpImpl(seeds, dra.ParseCqlOptions(shd.Attr)...)

	handler.duplfs = dra.NewDuplDra(handler.draOp)

	return handler, nil
}

// SeadraFile implements DFSFile
type SeadraFile struct {
	*weedfs.WeedFile

	handler *SeadraHandler
	meta    *meta.File

	md5  hash.Hash
	mode dfsFileMode
}

// GetFileInfo returns file meta info.
func (f SeadraFile) GetFileInfo() *transfer.FileInfo {
	return transfer.File2Info(f.meta)
}

// Read reads atmost len(p) bytes into p.
// Returns number of bytes read and an error if any.
func (f SeadraFile) Read(p []byte) (int, error) {
	return f.WeedFile.Read(p)
}

// Write writes len(p) bytes to the file.
// Returns number of bytes written and an error if any.
func (f SeadraFile) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	n, err := f.WeedFile.Write(p)
	if err != nil {
		return 0, err
	}

	f.md5.Write(p)
	f.meta.Size += int64(n)

	return n, nil
}

// Close closes an opened SeadraFile.
func (f SeadraFile) Close() error {
	if err := f.WeedFile.Close(); err != nil {
		return err
	}

	if f.mode == FileModeWrite {
		f.meta.UploadDate = time.Now()
		f.meta.Md5 = hex.EncodeToString(f.md5.Sum(nil))
		if err := f.handler.duplfs.Save(f.meta); err != nil {
			// try to remove the entity.
			weedfs.Remove(f.meta.ExtAttr[MetaKey_WeedFid], f.meta.Domain, f.handler.Shard.MasterUri)
			return err
		}

		glog.V(3).Infof("Save a seadra %v.", f.meta)
	}

	return nil
}

// updateFileMeta updates file dfs meta.
func (f SeadraFile) updateFileMeta(m map[string]interface{}) {
	f.meta.ExtAttr = make(map[string]string)
	for k, v := range m {
		f.meta.ExtAttr[k] = toString(v)
	}
}

// getFileMeta returns file dfs meta.
func (f SeadraFile) getFileMeta() *DFSFileMeta {
	return getFileMeta(f.meta)
}

func getFileMeta(m *meta.File) *DFSFileMeta {
	return &DFSFileMeta{
		Bizname:   m.Biz,
		ChunkSize: int64(m.ChunkSize),
		Fid:       m.ExtAttr[MetaKey_WeedFid],
	}
}

// hasEntity returns if the file has entity.
func (f SeadraFile) hasEntity() bool {
	return true
}
