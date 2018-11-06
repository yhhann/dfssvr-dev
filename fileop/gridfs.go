package fileop

import (
	"fmt"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
)

// GridFsHandler implements DFSFileHandler.
type GridFsHandler struct {
	*metadata.Shard

	session *mgo.Session
	gridfs  *mgo.GridFS
	duplfs  *DuplFs
}

func (h *GridFsHandler) copySessionAndGridFS() (*mgo.Session, *mgo.GridFS) {
	session, err := metadata.CopySession(h.Uri)
	if err != nil {
		glog.Errorf("Failed to copy session for %s, %v", h.Uri, err)
	}

	return session, session.DB(h.Shard.Name).GridFS("fs")
}

// releaseSession releases a session if err occured.
func (h *GridFsHandler) releaseSession(session *mgo.Session, err error) {
	if err != nil && session != nil {
		metadata.ReleaseSession(session)
	}
}

// ensureReleaseSession releases a session.
func (h *GridFsHandler) ensureReleaseSession(session *mgo.Session) {
	if session != nil {
		metadata.ReleaseSession(session)
	}
}

// Name returns handler's name.
func (h *GridFsHandler) Name() string {
	return h.Shard.Name
}

// Create creates a DFSFile for write with the given file info.
func (h *GridFsHandler) Create(info *transfer.FileInfo) (f DFSFile, err error) {
	session, gridfs := h.copySessionAndGridFS()
	defer func() {
		h.releaseSession(session, err)
	}()

	file, er := gridfs.Create(info.Name)
	if er != nil {
		err = er
		return
	}

	// For compatible with dfs 1.0.
	// This is a bug of driver in go, chunk size in java driver is 256k,
	// but in go is 255k. So we must reset it to 256k.
	file.SetChunkSize(256 * 1024)

	oid, ok := file.Id().(bson.ObjectId)
	if !ok {
		file.Close()
		err = fmt.Errorf("Invalid ObjectId, %T, %v", file.Id(), file.Id())
		return
	}

	if bson.IsObjectIdHex(info.Id) {
		oid = bson.ObjectIdHex(info.Id)
		file.SetId(oid)
	}

	// Make a copy of file info to hold information of file.
	inf := *info
	inf.Id = oid.Hex()

	f = &GridFsFile{
		GridFile: file,
		info:     &inf,
		handler:  h,
		mode:     FileModeWrite,
		session:  session,
		gridfs:   gridfs,
		meta:     make(map[string]interface{}),
	}

	return
}

// Open opens a DFSFile for read with given id and domain.
func (h *GridFsHandler) Open(id string, domain int64) (dfsFile DFSFile, err error) {
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

	inf := &transfer.FileInfo{
		Id:     id,
		Domain: domain,
		Name:   gridFile.Name(),
		Size:   gridFile.Size(),
		Md5:    gridFile.MD5(),
		Biz:    gridMeta.Bizname,
	}

	dfsFile = &GridFsFile{
		GridFile: gridFile,
		info:     inf,
		handler:  h,
		mode:     FileModeRead,
		session:  session,
		gridfs:   gridfs,
		meta:     make(map[string]interface{}),
	}

	return
}

// Duplicate duplicates an entry for a file.
func (h *GridFsHandler) Duplicate(oid string, domain int64) (string, error) {
	return h.duplfs.Duplicate(h.gridfs, oid)
}

// Find finds a file, if the file not exists, return empty string.
// If the file exists, return its file id.
// If the file exists and is a duplication, return its primitive file id.
func (h *GridFsHandler) Find(id string) (string, *DFSFileMeta, *transfer.FileInfo, error) {
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

// Remove deletes a file with its id and domain.
func (h *GridFsHandler) Remove(id string, domain int64) (bool, *meta.File, error) {
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
	}

	return result, m, nil
}

// Close releases resources the handler holds.
func (h *GridFsHandler) Close() error {
	h.session.Close()
	return nil
}

// HealthStatus returns the status of node health.
func (h *GridFsHandler) HealthStatus() int {
	if h.session.Ping() != nil {
		return MetaNotHealthy
	}

	return HealthOk
}

// FindByMd5 finds a file by its md5.
func (h *GridFsHandler) FindByMd5(md5 string, domain int64, size int64) (string, error) {
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

// NewGridFsHandler returns a handler for processing Grid files.
func NewGridFsHandler(shardInfo *metadata.Shard) (*GridFsHandler, error) {
	handler := &GridFsHandler{
		Shard: shardInfo,
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

// GridFsFile implements DFSFile.
type GridFsFile struct {
	*mgo.GridFile
	info    *transfer.FileInfo
	mode    dfsFileMode
	handler *GridFsHandler

	meta map[string]interface{}

	session *mgo.Session
	gridfs  *mgo.GridFS
}

// GetFileInfo returns file meta info.
func (f GridFsFile) GetFileInfo() *transfer.FileInfo {
	return f.info
}

func (f GridFsFile) updateFileMeta(m map[string]interface{}) {
	for k, v := range m {
		f.meta[k] = v
	}
}

func (f GridFsFile) getFileMeta() *DFSFileMeta {
	meta, err := getDFSFileMeta(f.GridFile)
	if err != nil {
		return nil
	}

	return meta
}

// Close closes GridFsFile.
func (f *GridFsFile) Close() error {
	defer func() {
		f.gridfs = nil
		if f.session != nil {
			metadata.ReleaseSession(f.session)
		}
	}()

	if f.mode == FileModeWrite {
		f.meta["bizname"] = f.info.Biz
		f.SetMeta(f.meta)
	}

	if err := f.GridFile.Close(); err != nil {
		return err
	}

	if f.mode == FileModeWrite {
		return f.updateGridMetadata()
	}
	return nil
}

func (f GridFsFile) updateGridMetadata() error {
	return f.gridfs.Files.Update(
		bson.M{
			"_id": bson.ObjectIdHex(f.info.Id),
		},
		bson.M{
			"$set": f.additionalMetadata(),
		},
	)
}

func (f GridFsFile) additionalMetadata() bson.D {
	var opdata bson.D

	opdata = append(opdata, bson.DocElem{
		"domain", f.info.Domain,
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

	return opdata
}

func getDFSFileMeta(g *mgo.GridFile) (*DFSFileMeta, error) {
	gridMeta := &DFSFileMeta{}
	if err := g.GetMeta(gridMeta); err != nil {
		return nil, err
	}

	return gridMeta, nil
}

// hasEntity returns if the file has entity.
func (f GridFsFile) hasEntity() bool {
	return true
}
