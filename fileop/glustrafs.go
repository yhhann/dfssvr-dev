package fileop

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/kshlm/gogfapi/gfapi"
	"gopkg.in/mgo.v2/bson"

	dra "jingoal.com/dfs/cassandra"
	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// GlustraHandler implements DFSFileHandler.
type GlustraHandler struct {
	*metadata.Shard
	*gfapi.Volume

	draOp  *dra.DraOpImpl
	duplfs meta.FileMetaOp

	VolLog string // Log file name of gluster volume
}

// Name returns handler's name.
func (h *GlustraHandler) Name() string {
	return h.Shard.Name
}

func (h *GlustraHandler) initLogDir() error {
	logDir := filepath.Dir(h.VolLog)

	_, err := os.Stat(logDir)
	if os.IsNotExist(err) {
		return os.MkdirAll(logDir, 0700)
	}
	if err != nil {
		return err
	}

	return nil
}

// initVolume initializes gluster volume.
func (h *GlustraHandler) initVolume() error {
	h.Volume = new(gfapi.Volume)

	if ret := h.Init(h.VolHost, h.VolName); ret != 0 {
		return fmt.Errorf("init volume %s on %s error: %d\n", h.VolName, h.VolHost, ret)
	}

	if err := h.initLogDir(); err != nil {
		return fmt.Errorf("Failed to create log directory: %v", err)
	}
	if ret, _ := h.SetLogging(h.VolLog, gfapi.LogInfo); ret != 0 {
		return fmt.Errorf("set log to %s error: %d\n", h.VolLog, ret)
	}

	if ret := h.Mount(); ret != 0 {
		return fmt.Errorf("mount %s error: %d\n", h.VolName, ret)
	}

	return nil
}

// Close releases resources.
func (h *GlustraHandler) Close() error {
	h.Unmount()
	return nil // For compatible with Unmount returns.
}

// Create creates a DFSFile for write.
func (h *GlustraHandler) Create(info *transfer.FileInfo) (DFSFile, error) {
	oid := bson.NewObjectId()
	if bson.IsObjectIdHex(info.Id) {
		oid = bson.ObjectIdHex(info.Id)
	}

	filePath := util.GetFilePath(h.VolBase, info.Domain, oid.Hex(), h.PathVersion, h.PathDigit)
	dir := filepath.Dir(filePath)
	if err := h.Volume.MkdirAll(dir, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}

	file, err := h.createGlustraFile(filePath)
	if err != nil {
		return nil, err
	}

	file.sdf = &meta.File{
		Id:        oid.Hex(),
		Biz:       info.Biz,
		Name:      info.Name,
		UserId:    fmt.Sprintf("%d", info.User),
		Domain:    info.Domain,
		ChunkSize: -1, // means no use.
		Type:      meta.EntityGlusterFS,
		ExtAttr:   make(map[string]string),
	}

	// Make a copy of file info to hold information of file.
	inf := *info
	inf.Id = file.sdf.Id
	inf.Size = file.sdf.Size
	file.info = &inf

	return file, nil
}

func (h *GlustraHandler) createGlustraFile(name string) (*GlustraFile, error) {
	f, err := h.Volume.Create(name)
	if err != nil {
		return nil, err
	}

	return &GlustraFile{
		glf:     f,
		md5:     md5.New(),
		mode:    FileModeWrite,
		handler: h,
	}, nil
}

// Open opens a file for read.
func (h *GlustraHandler) Open(id string, domain int64) (DFSFile, error) {
	f, err := h.duplfs.Find(id)
	if err != nil {
		return nil, err
	}

	filePath := util.GetFilePath(h.VolBase, f.Domain, f.Id, h.PathVersion, h.PathDigit)
	result, err := h.openGlustraFile(filePath)
	if err != nil {
		return nil, err
	}

	result.sdf = f
	result.info = &transfer.FileInfo{
		Id:     f.Id,
		Domain: f.Domain,
		Name:   f.Name,
		Size:   f.Size,
		Md5:    f.Md5,
		Biz:    f.Biz,
	}
	return result, nil
}

// Duplicate duplicates an entry for a file.
func (h *GlustraHandler) Duplicate(fid string, domain int64) (string, error) {
	return h.duplfs.DuplicateWithId(fid, "", time.Time{})
}

// Find finds a file. If the file not exists, return empty string.
// If the file exists and is a duplication, return its primitive file ID.
// If the file exists, return its file ID.
func (h *GlustraHandler) Find(id string) (string, *DFSFileMeta, *transfer.FileInfo, error) {
	f, err := h.duplfs.Find(id)
	if err != nil {
		return "", nil, nil, err
	}

	var chunksize int64 = 0
	chunksize, err = strconv.ParseInt(f.ExtAttr["chunksize"], 10, 64)
	if err != nil {
		glog.V(4).Infof("Failed to parse chunk size %s, %v.", f.ExtAttr["chunksize"], err)
	}

	meta := &DFSFileMeta{
		Bizname:   f.Biz,
		Fid:       f.ExtAttr[MetaKey_WeedFid],
		ChunkSize: chunksize,
	}

	var userId int64
	userId, err = strconv.ParseInt(f.UserId, 10, 64)
	if err != nil {
		glog.Warningf("Failed to parse user ID %s.", f.UserId)
	}

	info := &transfer.FileInfo{
		Id:     id,
		Name:   f.Name,
		Size:   f.Size,
		Md5:    f.Md5,
		Biz:    f.Biz,
		Domain: f.Domain,
		User:   userId,
	}

	glog.V(3).Infof("Succeeded to find file %s, return %s", id, f.Id)

	return f.Id, meta, info, nil
}

// Remove deletes file by its id and domain.
func (h *GlustraHandler) Remove(id string, domain int64) (bool, *meta.File, error) {
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

		filePath := util.GetFilePath(h.VolBase, domain, entityId, h.PathVersion, h.PathDigit)
		if err := h.Unlink(filePath); err != nil {
			glog.Warningf("Failed to remove file %s %d from %s, %s.", id, domain, h.Name(), err)
		}
	}

	return result, m, nil
}

func (h *GlustraHandler) openGlustraFile(name string) (*GlustraFile, error) {
	f, err := h.Volume.Open(name)
	if err != nil {
		return nil, err
	}

	return &GlustraFile{
		glf:     f,
		mode:    FileModeRead,
		handler: h,
	}, nil
}

// HealthStatus returns the status of node health.
func (h *GlustraHandler) HealthStatus() int {
	// check cassandra
	if err := h.draOp.HealthCheck(transfer.NodeName); err != nil {
		return MetaNotHealthy
	}

	magicDirPath := filepath.Join(h.VolBase, "health", transfer.ServerId)
	if err := h.Volume.MkdirAll(magicDirPath, 0755); err != nil && os.IsExist(err) {
		glog.Warningf("IsHealthy %s, error: %v", h.Name(), err)
		return StoreNotHealthy
	}

	fn := strconv.Itoa(int(time.Now().Unix()))
	magicFilePath := filepath.Join(magicDirPath, fn)
	if _, err := h.Volume.Create(magicFilePath); err != nil {
		glog.Warningf("IsHealthy %s, error: %v", h.Name(), err)
		return StoreNotHealthy
	}
	if err := h.Volume.Unlink(magicFilePath); err != nil {
		glog.Warningf("IsHealthy %s, error: %v", h.Name(), err)
		return StoreNotHealthy
	}

	return HealthOk
}

// FindByMd5 finds a file by its md5.
func (h *GlustraHandler) FindByMd5(md5 string, domain int64, size int64) (string, error) {
	file, err := h.duplfs.FindByMd5(md5, domain) // ignore size
	if err != nil {
		return "", err
	}

	return file.Id, nil
}

// Create creates a DFSFile with the given id.
func (h *GlustraHandler) CreateWithGivenId(info *transfer.FileInfo) (DFSFile, error) {
	return h.Create(info)
}

// Duplicate duplicates an entry with the given id.
func (h *GlustraHandler) DuplicateWithGivenId(primaryId string, dupId string) (string, error) {
	return h.duplfs.DuplicateWithId(primaryId, dupId, time.Time{})
}

// NewGlustraHandler creates a GlustraHandler.
func NewGlustraHandler(si *metadata.Shard, volLog string) (*GlustraHandler, error) {
	handler := &GlustraHandler{
		Shard:  si,
		VolLog: volLog,
	}

	if err := handler.initVolume(); err != nil {
		return nil, err
	}

	if si.ShdType != metadata.Glustra {
		return nil, fmt.Errorf("invalid shard type %d.", si.ShdType)
	}

	seeds := strings.Split(si.Uri, ",")
	handler.draOp = dra.NewDraOpImpl(seeds, dra.ParseCqlOptions(si.Attr)...)

	handler.duplfs = dra.NewDuplDra(handler.draOp)

	return handler, nil
}

// GlustraFile implements DFSFile
type GlustraFile struct {
	info    *transfer.FileInfo
	glf     *gfapi.File // Gluster file
	sdf     *meta.File
	md5     hash.Hash
	mode    dfsFileMode
	handler *GlustraHandler
}

// GetFileInfo returns file meta info.
func (f GlustraFile) GetFileInfo() *transfer.FileInfo {
	return f.info
}

// Read reads atmost len(p) bytes into p.
// Returns number of bytes read and an error if any.
func (f GlustraFile) Read(p []byte) (int, error) {
	nr, er := f.glf.Read(p)
	// When reached EOF, glf returns nr=0 other than er=io.EOF, fix it.
	if nr <= 0 {
		return 0, io.EOF
	}
	return nr, er
}

// Write writes len(p) bytes to the file.
// Returns number of bytes written and an error if any.
func (f GlustraFile) Write(p []byte) (int, error) {
	// If len(p) is zero, glf.Write() will panic.
	if len(p) == 0 { // fix bug of gfapi.
		return 0, nil
	}

	n, err := f.glf.Write(p)
	if err != nil {
		return 0, err
	}

	f.md5.Write(p)
	f.info.Size += int64(n)
	f.sdf.Size += int64(n)

	return n, nil
}

// Close closes an opened GlustraFile.
func (f GlustraFile) Close() error {
	if err := f.glf.Close(); err != nil {
		return err
	}

	if f.mode == FileModeWrite {
		f.sdf.UploadDate = time.Now()
		f.sdf.Md5 = hex.EncodeToString(f.md5.Sum(nil))
		if err := f.handler.duplfs.Save(f.sdf); err != nil {
			h := f.handler
			inf := f.info
			filePath := util.GetFilePath(h.VolBase, inf.Domain, inf.Id, h.PathVersion, h.PathDigit)
			if err := h.Unlink(filePath); err != nil {
				glog.Warningf("Failed to remove file without meta, %s %s %d from %s", inf.Id, inf.Name, inf.Domain, filePath)
			}

			return err
		}
	}

	return nil
}

// updateFileMeta updates file dfs meta.
func (f GlustraFile) updateFileMeta(m map[string]interface{}) {
	f.sdf.ExtAttr = make(map[string]string)
	for k, v := range m {
		f.sdf.ExtAttr[k] = toString(v)
	}
}

func toString(x interface{}) string {
	switch x := x.(type) {
	case nil:
		return "NULL"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", x)
	case float32, float64:
		return fmt.Sprintf("%g", x)
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case string:
		return x
	default:
		glog.Warningf("unexpected type %T: %v", x, x)
		return ""
	}
}

// getFileMeta returns file dfs meta.
func (f GlustraFile) getFileMeta() *DFSFileMeta {
	ck, err := strconv.ParseInt(f.sdf.ExtAttr[MetaKey_Chunksize], 10, 64)
	if err != nil {
		glog.Warningf("Failed to parse chunk size, %s", f.sdf.ExtAttr[MetaKey_Chunksize])
		ck = DefaultSeaweedChunkSize
	}
	return &DFSFileMeta{
		Bizname:   f.sdf.Biz,
		Fid:       f.sdf.ExtAttr[MetaKey_WeedFid],
		ChunkSize: ck,
	}
}

// hasEntity returns if the file has entity.
func (f GlustraFile) hasEntity() bool {
	return f.glf != nil
}
