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
	"time"

	"github.com/golang/glog"
	"github.com/kshlm/gogfapi/gfapi"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

const (
	VOLUME_PENDING int = 0
	VOLUME_OK      int = 1
)

// GlusterHandler implements DFSFileHandler.
type GlusterHandler struct {
	*metadata.Shard

	session *mgo.Session
	gridfs  *mgo.GridFS
	duplfs  *DuplFs

	*gfapi.Volume
	VolLog string // Log file name of gluster volume

	volumeState int
}

// Name returns handler's name.
func (h *GlusterHandler) Name() string {
	return h.Shard.Name
}

func (h *GlusterHandler) makeSureLogDir() error {
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
func (h *GlusterHandler) initVolume() error {
	h.Volume = new(gfapi.Volume)

	// for debug
	for glog.V(100) {
		time.Sleep(time.Second * 2)
	}

	if ret := h.Init(h.VolHost, h.VolName); ret != 0 {
		return fmt.Errorf("init volume %s on %s error: %d\n", h.VolName, h.VolHost, ret)
	}

	if err := h.makeSureLogDir(); err != nil {
		return fmt.Errorf("Failed to create log directory: %v", err)
	}
	if ret, _ := h.SetLogging(h.VolLog, gfapi.LogInfo); ret != 0 {
		return fmt.Errorf("set log to %s error: %d\n", h.VolLog, ret)
	}

	if ret := h.Mount(); ret != 0 {
		return fmt.Errorf("mount %s error: %d\n", h.VolName, ret)
	}

	h.volumeState = VOLUME_OK

	return nil
}

// Close releases resources.
func (h *GlusterHandler) Close() error {
	h.Unmount()
	return nil // For compatible with Unmount returns.
}

func (h *GlusterHandler) copySessionAndGridFS() (*mgo.Session, *mgo.GridFS) {
	session, err := metadata.CopySession(h.Uri)
	if err != nil {
		glog.Errorf("Failed to copy session for %s, %v", h.Uri, err)
	}

	return session, session.DB(h.Shard.Name).GridFS("fs")
}

// releaseSession releases a session if err occured.
func (h *GlusterHandler) releaseSession(session *mgo.Session, err error) {
	if err != nil && session != nil {
		metadata.ReleaseSession(session)
	}
}

// ensureReleaseSession releases a session.
func (h *GlusterHandler) ensureReleaseSession(session *mgo.Session) {
	if session != nil {
		metadata.ReleaseSession(session)
	}
}

// Create creates a DFSFile for write.
func (h *GlusterHandler) Create(info *transfer.FileInfo) (f DFSFile, err error) {
	session, gridfs := h.copySessionAndGridFS()
	defer func() {
		if _, ok := err.(RecoverableFileError); !ok {
			h.releaseSession(session, err)
		}
	}()

	gridFile, er := gridfs.Create(info.Name)
	if er != nil {
		err = er
		return
	}

	oid, ok := gridFile.Id().(bson.ObjectId)
	if !ok {
		gridFile.Close()
		err = fmt.Errorf("Invalid ObjectId, %T, %v", gridFile.Id(), gridFile.Id())
		return
	}

	if bson.IsObjectIdHex(info.Id) {
		oid = bson.ObjectIdHex(info.Id)
		gridFile.SetId(oid)
	}

	// For compatible with dfs 1.0.
	// This is a bug of driver in go, chunk size in java driver is 256k,
	// but in go is 255k. So we must reset it to 256k.
	gridFile.SetChunkSize(256 * 1024)

	// Make a copy of file info to hold information of file.
	inf := *info
	inf.Id = oid.Hex()
	inf.Size = 0

	file := &GlusterFile{
		md5:     md5.New(),
		mode:    FileModeWrite,
		handler: h,
		meta:    make(map[string]interface{}),
		grf:     gridFile,
		session: session,
		gridfs:  gridfs,
		info:    &inf,
	}
	f = file

	// for debug
	if glog.V(100) {
		err = RecoverableFileError{
			Code: GlusterFSFileError,
			Orig: fmt.Errorf("debug when creating"),
		}
		return
	}

	file.glf, err = h.CreateGlusterFile(file.info.Domain, file.info.Id)
	return
}

func (h *GlusterHandler) checkVolume() error {
	if h.volumeState == VOLUME_PENDING {
		return RecoverableFileError{
			Code: GlusterFSFileError,
			Orig: fmt.Errorf("volume pending %s", h.Name()),
		}
	}

	return nil
}

func (h *GlusterHandler) CreateGlusterFile(domain int64, fid string) (*gfapi.File, error) {
	filePath := util.GetFilePath(h.VolBase, domain, fid, h.PathVersion, h.PathDigit)
	dir := filepath.Dir(filePath)

	if err := h.checkVolume(); err != nil {
		return nil, err
	}

	if err := h.Volume.MkdirAll(dir, 0755); err != nil && !os.IsExist(err) {
		glog.Warningf("Failed to create glusterfs dir, %s, %v", dir, err)
		return nil, RecoverableFileError{
			Code: GlusterFSFileError,
			Orig: err,
		}
	}

	glf, err := h.Volume.Create(filePath)
	if err != nil {
		glog.Warningf("Failed to create glusterfs file, %s, %v", filePath, err)
		return nil, RecoverableFileError{
			Code: GlusterFSFileError,
			Orig: err,
		}
	}

	return glf, nil
}

// Open opens a file for read.
func (h *GlusterHandler) Open(id string, domain int64) (f DFSFile, err error) {
	if err := h.checkVolume(); err != nil {
		return nil, err
	}

	session, gridfs := h.copySessionAndGridFS()
	defer func() {
		h.releaseSession(session, err)
	}()

	gridFile, er := h.duplfs.Find(gridfs, id)
	if er != nil {
		err = er
		return
	}

	gridMeta, err := getDFSFileMeta(gridFile)
	if err != nil {
		return
	}

	oid, ok := gridFile.Id().(bson.ObjectId)
	if !ok {
		err = fmt.Errorf("assertion error %T %v", gridFile.Id(), gridFile.Id())
		return
	}

	filePath := util.GetFilePath(h.VolBase, domain, oid.Hex(), h.PathVersion, h.PathDigit)
	result, er := h.openGlusterFile(filePath)
	if er != nil {
		err = er
		return
	}

	result.grf = gridFile
	result.session = session
	result.gridfs = gridfs
	result.info = &transfer.FileInfo{
		Id:     id,
		Domain: domain,
		Name:   gridFile.Name(),
		Size:   gridFile.Size(),
		Md5:    gridFile.MD5(),
		Biz:    gridMeta.Bizname,
	}
	f = result

	return
}

// Duplicate duplicates an entry for a file.
func (h *GlusterHandler) Duplicate(oid string, domain int64) (string, error) {
	return h.duplfs.Duplicate(h.gridfs, oid)
}

// Find finds a file, if the file not exists, return empty string.
// If the file exists, return its file id.
// If the file exists and is a duplication, return its primitive file id.
func (h *GlusterHandler) Find(id string) (string, *DFSFileMeta, *transfer.FileInfo, error) {
	session, gridfs := h.copySessionAndGridFS()
	defer func() {
		h.ensureReleaseSession(session)
	}()

	gridFile, err := h.duplfs.Find(gridfs, id)
	if err == mgo.ErrNotFound {
		return "", nil, nil, nil
	}
	if err != nil {
		return "", nil, nil, err
	}
	defer gridFile.Close()

	oid, ok := gridFile.Id().(bson.ObjectId)
	if !ok {
		return "", nil, nil, fmt.Errorf("find file error %s", id)
	}

	meta, err := getDFSFileMeta(gridFile)
	if err != nil {
		return "", nil, nil, err
	}

	info := &transfer.FileInfo{
		Id:   id,
		Name: gridFile.Name(),
		Size: gridFile.Size(),
		Md5:  gridFile.MD5(),
		Biz:  meta.Bizname,
		// TODO(hanyh): add Domain and User
	}

	glog.V(3).Infof("Succeeded to find file %s, return %s", id, oid.Hex())

	return oid.Hex(), meta, info, nil
}

// Remove deletes file by its id and domain.
func (h *GlusterHandler) Remove(id string, domain int64) (bool, *meta.File, error) {
	if err := h.checkVolume(); err != nil {
		return false, nil, err
	}

	session, gridfs := h.copySessionAndGridFS()
	defer func() {
		h.ensureReleaseSession(session)
	}()

	result, entityId, err := h.duplfs.LazyDelete(gridfs, id)
	if err != nil {
		glog.Warningf("Failed to remove file %s %d from %s, %s.", id, domain, h.Name(), err)
		return false, nil, err
	}

	var m *meta.File
	if result {
		query := bson.D{
			{"_id", *entityId},
		}
		m, err = LookupFileMeta(gridfs, query)
		if err != nil {
			return false, nil, err
		}
		removeEntity(gridfs, *entityId)

		filePath := util.GetFilePath(h.VolBase, domain, (*entityId).Hex(), h.PathVersion, h.PathDigit)
		if err := h.Unlink(filePath); err != nil {
			glog.Warningf("Failed to remove file %s %d from %s, %s.", id, domain, h.Name(), err)
		}
	}

	return result, m, nil
}

func (h *GlusterHandler) openGlusterFile(name string) (*GlusterFile, error) {
	f, err := h.Volume.Open(name)
	if err != nil {
		return nil, err
	}

	return &GlusterFile{
		glf:     f,
		mode:    FileModeRead,
		handler: h,
		meta:    make(map[string]interface{}),
	}, nil
}

// HealthStatus returns the status of node health.
func (h *GlusterHandler) HealthStatus() int {
	if err := h.checkVolume(); err != nil {
		return StoreNotHealthy
	}

	if err := h.session.Ping(); err != nil {
		return MetaNotHealthy
	}

	magicDirPath := filepath.Join(h.VolBase, "health", transfer.ServerId)
	if err := h.Volume.MkdirAll(magicDirPath, 0755); err != nil && !os.IsExist(err) {
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
func (h *GlusterHandler) FindByMd5(md5 string, domain int64, size int64) (string, error) {
	file, err := h.duplfs.FindByMd5(h.gridfs, md5, domain, size)
	if err != nil {
		return "", err
	}

	oid, ok := file.Id().(bson.ObjectId)
	if !ok {
		return "", fmt.Errorf("Invalid id, %T, %v", file.Id(), file.Id())
	}

	return oid.Hex(), nil
}

// NewGlusterHandler creates a GlusterHandler.
func NewGlusterHandler(shardInfo *metadata.Shard, volLog string) (*GlusterHandler, error) {
	handler := &GlusterHandler{
		Shard:  shardInfo,
		VolLog: volLog,
	}

	volumeSignal := make(chan struct{})
	t := time.NewTimer(2 * time.Second)

	go func() {
		start := time.Now()
		for err := handler.initVolume(); err != nil; {
			instrument.VolumeInitError.WithLabelValues(handler.Name()).Inc()
			glog.Errorf("Failed to initialize volume %s, elapse %v, %v.", handler.Name(), time.Since(start), err)
			time.Sleep(5 * time.Second)
		}

		instrument.VolumeInitError.WithLabelValues(handler.Name()).Set(0.0)
		glog.Infof("Succeeded to initialize volume %s, elapse %v.", handler.Name(), time.Since(start))
		close(volumeSignal)
	}()

	select {
	case <-t.C:
		glog.Warningf("Volume %s is pending...", handler.Name())
	case <-volumeSignal:
	}

	session, err := metadata.CopySession(shardInfo.Uri)
	if err != nil {
		return nil, err
	}

	handler.session = session
	handler.gridfs = session.DB(handler.Shard.Name).GridFS("fs")

	duplOp, err := metadata.NewDuplicateOp(shardInfo.Name, shardInfo.Uri, "fs")
	if err != nil {
		return nil, err
	}

	handler.duplfs = NewDuplFs(duplOp)

	return handler, nil
}

// GlusterFile implements DFSFile
type GlusterFile struct {
	info    *transfer.FileInfo
	glf     *gfapi.File   // Gluster file
	grf     *mgo.GridFile // GridFile
	md5     hash.Hash
	mode    dfsFileMode
	handler *GlusterHandler

	meta map[string]interface{}

	session *mgo.Session
	gridfs  *mgo.GridFS
}

// GetFileInfo returns file meta info.
func (f GlusterFile) GetFileInfo() *transfer.FileInfo {
	return f.info
}

func (f GlusterFile) updateFileMeta(m map[string]interface{}) {
	for k, v := range m {
		f.meta[k] = v
	}
}

func (f GlusterFile) getFileMeta() *DFSFileMeta {
	m, err := getDFSFileMeta(f.grf)
	if err != nil {
		return nil
	}

	return m
}

// Read reads atmost len(p) bytes into p.
// Returns number of bytes read and an error if any.
func (f GlusterFile) Read(p []byte) (int, error) {
	if !f.hasEntity() {
		return 0, NoEntityError
	}

	nr, er := f.glf.Read(p)
	// When reached EOF, glf returns nr=0 other than er=io.EOF, fix it.
	if nr <= 0 {
		return 0, io.EOF
	}
	return nr, er
}

// Write writes len(p) bytes to the file.
// Returns number of bytes written and an error if any.
func (f GlusterFile) Write(p []byte) (int, error) {
	n := 0
	if f.hasEntity() {
		// if len(p) is zero, glf.Write() will panic.
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

	f.info.Size += int64(n)
	f.md5.Write(p)

	return n, nil
}

// Close closes an open GlusterFile.
// Returns an error on failure.
func (f GlusterFile) Close() error {
	defer func() {
		f.gridfs = nil
		if f.session != nil {
			metadata.ReleaseSession(f.session)
		}
	}()

	if f.hasEntity() {
		if err := f.glf.Close(); err != nil {
			return err
		}
	}

	if f.mode == FileModeWrite {
		f.meta["bizname"] = f.info.Biz
		f.grf.SetMeta(f.meta)
	}

	if err := f.grf.Close(); err != nil {
		return err
	}

	if f.mode == FileModeWrite {
		return f.updateMetadata()
	}

	return nil
}

func (f GlusterFile) updateMetadata() error {
	return f.gridfs.Files.Update(
		bson.M{
			"_id": f.grf.Id(),
		},
		bson.M{
			"$set": f.additionalMetadata(),
		},
	)
}

func (f GlusterFile) additionalMetadata() bson.D {
	var opdata bson.D

	opdata = append(opdata, bson.DocElem{
		"domain", f.info.Domain,
	})
	opdata = append(opdata, bson.DocElem{
		"length", f.info.Size,
	})
	opdata = append(opdata, bson.DocElem{
		"userid", fmt.Sprintf("%d", f.info.User),
	})
	opdata = append(opdata, bson.DocElem{
		"bizname", f.info.Biz, // For compatible with dfs 1.0
	})
	opdata = append(opdata, bson.DocElem{
		"contentType", nil, // For compatible with dfs 1.0
	})
	opdata = append(opdata, bson.DocElem{
		"aliases", nil, // For compatible with dfs 1.0
	})
	opdata = append(opdata, bson.DocElem{
		"md5", hex.EncodeToString(f.md5.Sum(nil)),
	})

	return opdata
}

// hasEntity returns if the file has entity.
func (f GlusterFile) hasEntity() bool {
	return f.glf != nil
}
