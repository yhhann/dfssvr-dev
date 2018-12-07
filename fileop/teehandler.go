package fileop

import (
	"io"

	"github.com/golang/glog"

	"jingoal.com/dfs/conf"
	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/proto/transfer"
)

type TeeHandler struct {
	major DFSFileHandler
	minor DFSFileMinorHandler
}

// Create creates a DFSFile for write
func (h *TeeHandler) Create(info *transfer.FileInfo) (DFSFile, error) {
	tf := &TeeFile{}

	var err error
	tf.majorFile, err = h.major.Create(info)
	if err != nil {
		glog.Warningf("Failed to create file %v on major %s, %v.", info, h.Name(), err)
		return tf, err
	}
	f := tf.majorFile

	glog.V(2).Infof("Create file %v on major %s.", f.GetFileInfo().Id, h.Name())

	if conf.IsMinorWriteOk(info.Domain) {
		f, err := h.minor.CreateWithGivenId(f.GetFileInfo())
		if err != nil {
			instrument.MinorFileCounter <- &instrument.Measurements{
				Name:  "create_failed",
				Value: 1.0,
			}
			glog.Warningf("Failed to create file %v on minor %s, %v.", info, h.Name(), err)
		} else {
			instrument.MinorFileCounter <- &instrument.Measurements{
				Name:  "created",
				Value: 1.0,
			}
			glog.V(2).Infof("Create file %v on minor %s.", f.GetFileInfo().Id, h.Name())
		}

		tf.minorFile = f
	}

	return tf, err
}

// Open opens a DFSFile for read
func (h *TeeHandler) Open(id string, domain int64) (DFSFile, error) {
	var err error
	tf := &TeeFile{}

	// Try to open file on minor if any.
	if conf.IsMinorReadOk(domain) {
		tf.minorFile, err = h.minor.Open(id, domain)
		if err != nil {
			instrument.MinorFileCounter <- &instrument.Measurements{
				Name:  "open_failed",
				Value: 1.0,
			}
			glog.Warningf("Failed to open file %v on minor %s, %v.", tf.GetFileInfo(), h.Name(), err)
		} else {
			instrument.MinorFileCounter <- &instrument.Measurements{
				Name:  "opened",
				Value: 1.0,
			}
			glog.V(2).Infof("Open file %v on minor %s.", tf.GetFileInfo().Id, h.Name())

			// How to deal with 'file not found'?
			return tf, nil
		}
	}

	tf.majorFile, err = h.major.Open(id, domain)
	if err != nil {
		glog.Warningf("Failed to open file %v on major %s, %v.", tf.GetFileInfo(), h.Name(), err)
		return nil, err
	}

	glog.V(2).Infof("Open file %v on minor %s.", tf.GetFileInfo().Id, h.Name())
	return tf, nil
}

// Duplicate duplicates an entry for a file.
func (h *TeeHandler) Duplicate(oid string, domain int64) (string, error) {
	did, err := h.major.Duplicate(oid, domain)
	if err != nil {
		return did, err
	}

	if conf.IsMinorWriteOk(domain) {
		_, err = h.minor.DuplicateWithGivenId(oid, did)
		if err != nil {
			instrument.MinorFileCounter <- &instrument.Measurements{
				Name:  "duplicate_failed",
				Value: 1.0,
			}
			glog.Warningf("Failed to duplicate file %s/%s on minor %s, %v.", did, oid, h.Name(), err)
		} else {
			instrument.MinorFileCounter <- &instrument.Measurements{
				Name:  "duplicated",
				Value: 1.0,
			}

			glog.V(2).Infof("Duplicate file %s/%s on minor %s.", did, oid, h.Name())
		}
	}

	return did, err
}

// Remove deletes a file by its id.
func (h *TeeHandler) Remove(id string, domain int64) (bool, *meta.File, error) {
	result, metadata, err := h.major.Remove(id, domain)
	if err != nil {
		return result, metadata, err
	}

	_, _, err = h.minor.Remove(id, domain)
	if err == meta.FileNotFound {
		glog.V(2).Infof("Remove file %s from minor %s, %v", id, h.Name(), err)
		return result, metadata, nil
	}
	if err != nil {
		instrument.MinorFileCounter <- &instrument.Measurements{
			Name:  "remove_failed",
			Value: 1.0,
		}
		glog.Warningf("Failed to remove file %s on minor %s, %v.", id, h.Name(), err)
		return result, metadata, err
	}

	instrument.MinorFileCounter <- &instrument.Measurements{
		Name:  "removed",
		Value: 1.0,
	}
	glog.V(2).Infof("Remove file %s from minor %s.", id, h.Name())
	return result, metadata, nil
}

// Find finds a file, if the file not exists, return empty string.
// If the file exists, return its file id.
// If the file exists and is a duplication, return its primitive file id.
func (h *TeeHandler) Find(fid string) (string, *DFSFileMeta, *transfer.FileInfo, error) {
	id, m, info, err := h.minor.Find(fid)
	if err == nil {
		glog.V(2).Infof("Find file %s from minor %s.", fid, h.Name())
		return id, m, info, err
	}

	return h.major.Find(fid)
}

// FindByMd5 finds a file by its md5.
func (h *TeeHandler) FindByMd5(md5 string, domain int64, size int64) (string, error) {
	if conf.IsMinorReadOk(domain) {
		fid, err := h.minor.FindByMd5(md5, domain, size)
		if err == nil {
			return fid, nil
		}
	}

	return h.major.FindByMd5(md5, domain, size)
}

// Name returns handler's name.
func (h *TeeHandler) Name() string {
	return h.major.Name()
}

// HealthStatus returns the status of node health.
func (h *TeeHandler) HealthStatus() int {
	// TODO(hanyh): check minor.
	return h.major.HealthStatus()
}

// Close releases resources the handler holds.
func (h *TeeHandler) Close() (err error) {
	if h.major != nil {
		err = h.major.Close()
		h.major = nil
	}

	if h.minor != nil {
		err = h.minor.Close()
		h.minor = nil
	}

	return err
}

func (h *TeeHandler) GetMajor() DFSFileHandler {
	return h.major
}

func NewTeeHandler(majorHandler DFSFileHandler, minorHandler DFSFileMinorHandler) *TeeHandler {
	return &TeeHandler{
		major: majorHandler,
		minor: minorHandler,
	}
}

type TeeFile struct {
	majorFile DFSFile
	minorFile DFSFile
}

// GetFileInfo returns file info.
func (f *TeeFile) GetFileInfo() *transfer.FileInfo {
	if f.minorFile != nil {
		return f.minorFile.GetFileInfo()
	}
	return f.majorFile.GetFileInfo()
}

// updateFileMeta updates file dfs meta.
func (f *TeeFile) updateFileMeta(attrs map[string]interface{}) {
	if f.majorFile != nil {
		f.majorFile.updateFileMeta(attrs)
	}

	if f.minorFile != nil {
		f.minorFile.updateFileMeta(attrs)
	}
}

// getFileMeta returns file dfs meta.
func (f *TeeFile) getFileMeta() *DFSFileMeta {
	if f.minorFile != nil {
		return f.minorFile.getFileMeta()
	}
	return f.majorFile.getFileMeta()
}

// Write writes a byte buffer into tee file.
func (f *TeeFile) Write(p []byte) (n int, err error) {
	n, err = f.majorFile.Write(p)
	if err != nil { // strict
		return n, err
	}

	if f.minorFile != nil {
		_, er := f.minorFile.Write(p)
		if er != nil {
			f.minorFile = nil
			glog.Warningf("Failed to write to minor %s.", er)
		}
	}

	return
}

// Read reads a byte buffer from tee file.
func (f *TeeFile) Read(p []byte) (n int, err error) {
	if f.minorFile != nil {
		n, err = f.minorFile.Read(p)
		if err != nil && err != io.EOF {
			glog.Warningf("Failed to read from minor %s.", err)
		}
		return
	}

	return f.majorFile.Read(p)
}

// Close closes a tee file.
func (f *TeeFile) Close() (err error) {
	if f.minorFile != nil {
		err = f.minorFile.Close()
		if err != nil {
			glog.Warningf("Failed to close minor file %s, %v.", f.GetFileInfo().Id, err)
		}
	}

	glog.V(2).Infof("Close minor file %s.", f.GetFileInfo().Id)

	if f.majorFile != nil {
		err = f.majorFile.Close()
	}

	return
}

// hasEntity returns if the file has entity.
func (f *TeeFile) hasEntity() bool {
	return true
}
