package fileop

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/kshlm/gogfapi/gfapi"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/sql"
	"jingoal.com/dfs/util"
)

var (
	glustiTest = flag.Bool("glusti-test", true, "test for glusti")
)

// GlustiHandler implements DFSFileHandler.
type GlustiHandler struct {
	*metadata.Shard

	tiop meta.FileMetaOp

	*gfapi.Volume
	VolLog string // Log file name of gluster volume
}

// Name returns handler's name.
func (h *GlustiHandler) Name() string {
	return h.Shard.Name
}

func (h *GlustiHandler) initLogDir() error {
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
func (h *GlustiHandler) initVolume() error {
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
func (h *GlustiHandler) Close() error {
	h.Unmount()
	return nil // For compatible with Unmount returns.
}

// Create creates a DFSFile for write.
func (h *GlustiHandler) Create(info *transfer.FileInfo) (DFSFile, error) {
	oid := bson.NewObjectId()
	if bson.IsObjectIdHex(info.Id) {
		oid = bson.ObjectIdHex(info.Id)
	}

	filePath := util.GetFilePath(h.VolBase, info.Domain, oid.Hex(), h.PathVersion, h.PathDigit)
	file, err := h.createGlustiFile(filePath)
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

	glog.V(2).Infof("Succeeded to create file %s, from %s.", inf.Id, h.Name())
	return file, nil
}

func (h *GlustiHandler) createGlustiFile(name string) (*GlustiFile, error) {
	if *glustiTest {
		return &GlustiFile{
			glf:     nil,
			md5:     md5.New(),
			mode:    FileModeWrite,
			handler: h,
		}, nil
	}

	dir := filepath.Dir(name)
	if err := h.Volume.MkdirAll(dir, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}

	f, err := h.Volume.Create(name)
	if err != nil {
		return nil, err
	}

	return &GlustiFile{
		glf:     f,
		md5:     md5.New(),
		mode:    FileModeWrite,
		handler: h,
	}, nil
}

// Open opens a file for read.
func (h *GlustiHandler) Open(id string, domain int64) (DFSFile, error) {
	fm, err := h.tiop.Find(id)
	if err != nil {
		return nil, err
	}

	filePath := util.GetFilePath(h.VolBase, fm.Domain, fm.Id, h.PathVersion, h.PathDigit)
	f, err := h.openGlustiFile(filePath)
	if err != nil {
		return nil, err
	}

	f.sdf = fm
	f.info = &transfer.FileInfo{
		Id:     fm.Id,
		Domain: fm.Domain,
		Name:   fm.Name,
		Size:   fm.Size,
		Md5:    fm.Md5,
		Biz:    fm.Biz,
	}

	glog.V(2).Infof("Succeeded to open file %s, from %s.", id, h.Name())
	return f, nil
}

// Duplicate duplicates an entry for a file.
func (h *GlustiHandler) Duplicate(fid string, domain int64) (string, error) {
	return h.tiop.DuplicateWithId(fid, "", time.Time{})
}

// Find finds a file. If the file not exists, return empty string.
// If the file exists and is a duplication, return its primitive file ID.
// If the file exists, return its file ID.
func (h *GlustiHandler) Find(id string) (string, *DFSFileMeta, *transfer.FileInfo, error) {
	f, err := h.tiop.Find(id)
	if err != nil {
		return "", nil, nil, err
	}

	var chunksize int64 = 0
	chunksize, err = strconv.ParseInt(f.ExtAttr["chunksize"], 10, 64)
	if err != nil {
		glog.V(4).Infof("Chunk size can't be parsed %s, %v.", f.ExtAttr["chunksize"], err)
		chunksize = -1
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

	glog.V(2).Infof("Succeeded to find file %s, entity %s, from %s.", id, f.Id, h.Name())

	return f.Id, meta, info, nil
}

// Remove deletes file by its id and domain.
func (h *GlustiHandler) Remove(id string, domain int64) (bool, *meta.File, error) {
	f, err := h.tiop.Find(id)
	if err != nil {
		return false, nil, err
	}

	result, entityId, err := h.tiop.Delete(id)
	if err != nil {
		glog.Warningf("Failed to remove file %s %d from %s, %v.", id, domain, h.Name(), err)
		return false, nil, err
	}

	glog.V(2).Infof("Succeeded to remove file %s %d, from %s.", id, domain, h.Name())

	// TODO(hanyh):
	// assert entity should equals to f.Id

	if result && !*glustiTest {
		filePath := util.GetFilePath(h.VolBase, domain, entityId, h.PathVersion, h.PathDigit)
		if err := h.Unlink(filePath); err != nil {
			glog.Warningf("Failed to remove file %s %d from %s, %s.", id, domain, h.Name(), err)
		}
	}

	return result, f, nil
}

func (h *GlustiHandler) openGlustiFile(name string) (*GlustiFile, error) {
	f, err := h.Volume.Open(name)
	if err != nil {
		return nil, err
	}

	return &GlustiFile{
		glf:     f,
		mode:    FileModeRead,
		handler: h,
	}, nil
}

// HealthStatus returns the status of node health.
func (h *GlustiHandler) HealthStatus() int {
	// metadata storage ignored

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
func (h *GlustiHandler) FindByMd5(md5 string, domain int64, size int64) (string, error) {
	file, err := h.tiop.FindByMd5(md5, domain) // ignore size
	if err != nil {
		return "", err
	}

	glog.V(2).Infof("Succeeded to find by md5 %s %d, from %s.", md5, domain, h.Name())

	return file.Id, nil
}

// Create creates a DFSFile with the given id.
func (h *GlustiHandler) CreateWithGivenId(info *transfer.FileInfo) (DFSFile, error) {
	return h.Create(info)
}

// Duplicate duplicates an entry with the given id.
func (h *GlustiHandler) DuplicateWithGivenId(primaryId string, dupId string) (string, error) {
	return h.tiop.DuplicateWithId(primaryId, dupId, time.Time{})
}

// InitVolumeCB is a callback function invoked by major to initialize volume.
func (h *GlustiHandler) InitVolumeCB(host, name, base string) error {
	h.Shard.VolHost = host
	h.Shard.VolName = name
	h.Shard.VolBase = base

	glog.V(2).Infof("Initial volume by callback %s %s %s", host, name, base)
	return h.initVolume()
}

// NewGlustiHandler creates a GlustiHandler.
func NewGlustiHandler(si *metadata.Shard, volLog string) (*GlustiHandler, error) {
	handler := &GlustiHandler{
		Shard:  si,
		VolLog: volLog,
	}

	if !*glustiTest {
		if err := handler.initVolume(); err != nil {
			return nil, err
		}
	}

	if si.ShdType != metadata.Glusti {
		return nil, fmt.Errorf("invalid shard type %d.", si.ShdType)
	}

	dsns, err := sql.ConvertDSN(si.Uri)
	if err != nil {
		return nil, err
	}

	mgr := sql.NewDatabaseMgr(dsns)
	handler.tiop = sql.NewTiDBMetaImpl(mgr)

	return handler, nil
}

// GlustiFile implements DFSFile
type GlustiFile struct {
	info    *transfer.FileInfo
	glf     *gfapi.File // Gluster file
	sdf     *meta.File
	md5     hash.Hash
	mode    dfsFileMode
	handler *GlustiHandler
}

// GetFileInfo returns file meta info.
func (f GlustiFile) GetFileInfo() *transfer.FileInfo {
	return f.info
}

// Read reads atmost len(p) bytes into p.
// Returns number of bytes read and an error if any.
func (f GlustiFile) Read(p []byte) (int, error) {
	nr, er := f.glf.Read(p)
	// When reached EOF, glf returns nr=0 other than er=io.EOF, fix it.
	if nr <= 0 {
		return 0, io.EOF
	}
	return nr, er
}

// Write writes len(p) bytes to the file.
// Returns number of bytes written and an error if any.
func (f GlustiFile) Write(p []byte) (int, error) {
	n := 0
	if f.hasEntity() {
		// If len(p) is zero, glf.Write() will panic.
		if len(p) == 0 { // fix bug of gfapi.
			return 0, nil
		}

		var err error
		n, err = f.glf.Write(p)
		if err != nil {
			return 0, err
		}
	} else {
		n = len(p)
	}

	f.md5.Write(p)
	f.info.Size += int64(n)
	f.sdf.Size += int64(n)

	return n, nil
}

// Close closes an opened GlustiFile.
func (f GlustiFile) Close() error {
	if f.hasEntity() {
		if err := f.glf.Close(); err != nil {
			return err
		}
	}

	if f.mode == FileModeWrite {
		f.sdf.UploadDate = time.Now()
		f.sdf.Md5 = hex.EncodeToString(f.md5.Sum(nil))
		if err := f.handler.tiop.Save(f.sdf); err != nil {
			glog.Warningf("Failed to save metadata to tidb, %v", err)

			if f.hasEntity() {
				h := f.handler
				inf := f.info
				filePath := util.GetFilePath(h.VolBase, inf.Domain, inf.Id, h.PathVersion, h.PathDigit)
				if err := h.Unlink(filePath); err != nil {
					glog.Warningf("Failed to remove file without meta, %s %s %d from %s", inf.Id, inf.Name, inf.Domain, filePath)
				}
			}

			return err
		}
	}

	glog.V(2).Infof("Succeeded to close file %s %d.", f.sdf.Id, f.sdf.Domain)
	return nil
}

// updateFileMeta updates file dfs meta.
func (f GlustiFile) updateFileMeta(m map[string]interface{}) {
	f.sdf.ExtAttr = make(map[string]string)
	for k, v := range m {
		f.sdf.ExtAttr[k] = toString(v)
	}
}

// getFileMeta returns file dfs meta.
func (f GlustiFile) getFileMeta() *DFSFileMeta {
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
func (f GlustiFile) hasEntity() bool {
	return f.glf != nil
}
