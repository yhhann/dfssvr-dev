package fileop

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/util"
)

type DuplFs struct {
	*metadata.DuplicateOp
}

// FindByMd5 finds a gridfs file by its md5.
func (duplfs *DuplFs) FindByMd5(gridfs *mgo.GridFS, md5 string, domain int64, size int64) (*mgo.GridFile, error) {
	file := new(mgo.GridFile)

	query := bson.D{
		{"domain", domain},
		{"md5", md5},
		{"length", size},
	}

	iter := gridfs.Find(query).Sort("-uploadDate").Iter()
	defer iter.Close()

	if ok := gridfs.OpenNext(iter, &file); ok {
		return file, nil
	}

	return nil, meta.FileNotFound
}

// Find finds a file with given id.
func (duplfs *DuplFs) Find(gridfs *mgo.GridFS, givenId string) (f *mgo.GridFile, err error) {
	defer func() {
		if err == mgo.ErrNotFound {
			err = meta.FileNotFound
		}
	}()

	realId, err := hexString2ObjectId(util.GetRealId(givenId))
	if err != nil {
		return
	}

	if !util.IsDuplId(givenId) {
		f, err = gridfs.OpenId(*realId)
		return
	}

	dupl, err := duplfs.LookupDuplById(*realId)
	if err != nil {
		return
	}
	if dupl != nil {
		f, err = gridfs.OpenId(dupl.Ref)
		return
	}

	err = meta.FileNotFound
	return
}

func (duplfs *DuplFs) search(gridfs *mgo.GridFS, fid string) (*mgo.GridFile, error) {
	if !util.IsDuplId(fid) {
		givenId, err := hexString2ObjectId(fid)
		if err != nil {
			return nil, err
		}

		return gridfs.OpenId(givenId)
	}

	rId := util.GetRealId(fid)
	realId, err := hexString2ObjectId(rId)
	if err != nil {
		return nil, err
	}

	dupl, err := duplfs.LookupDuplById(*realId)
	if err != nil {
		return nil, err
	}
	if dupl == nil || !dupl.Ref.Valid() {
		return nil, meta.FileNotFound
	}

	return gridfs.OpenId(dupl.Ref)
}

// Duplicate duplicates an entry for a file, not the content.
func (duplfs *DuplFs) Duplicate(gridfs *mgo.GridFS, oid string) (string, error) {
	return duplfs.DuplicateWithId(gridfs, oid, "", time.Now())
}

func (duplfs *DuplFs) saveRefAndDuplIfAbsent(pid bson.ObjectId, size int64) (*metadata.Ref, error) {
	ref, err := duplfs.LookupRefById(pid)
	if err != nil {
		return nil, err
	}

	if ref != nil {
		return ref, nil
	}

	ref = &metadata.Ref{
		Id:     pid,
		Length: size,
		RefCnt: 0,
	}
	if err := duplfs.SaveRef(ref); err != nil {
		return nil, err
	}

	nDupl := metadata.Dupl{
		Id:     pid,
		Ref:    ref.Id,
		Length: size,
	}
	if err := duplfs.SaveDupl(&nDupl); err != nil {
		return nil, err
	}

	return ref, nil
}

// DuplicateWithId duplicates an entry for a file with given file id, not the content.
func (duplfs *DuplFs) DuplicateWithId(gridfs *mgo.GridFS, oid string, dupId string, uploadDate time.Time) (string, error) {
	primary, err := duplfs.search(gridfs, oid)
	if err != nil {
		return "", err
	}

	pid, ok := primary.Id().(bson.ObjectId)
	if !ok {
		return "", fmt.Errorf("primary id invalided: %v", primary.Id())
	}

	ref, err := duplfs.saveRefAndDuplIfAbsent(pid, primary.Size())
	if err != nil {
		return "", err
	}
	_, err = duplfs.IncRefCnt(ref.Id)
	if err != nil {
		return "", err
	}

	dupl := metadata.Dupl{
		Ref:    ref.Id,
		Length: ref.Length,
	}

	if dupId != "" {
		dupHex, err := hexString2ObjectId(dupId)
		if err != nil {
			return "", err
		}
		dupl.Id = *dupHex
	}

	dupl.UploadDate = uploadDate

	if err := duplfs.SaveDupl(&dupl); err != nil {
		return "", err
	}

	return util.GetDuplId(dupl.Id.Hex()), nil
}

// LazyDelete deletes a duplication or a real file.
// It returns true when deletes a real file successfully.
func (duplfs *DuplFs) LazyDelete(gridfs *mgo.GridFS, dId string) (bool, *bson.ObjectId, error) {
	var status int64
	var result bool

	realId, err := hexString2ObjectId(util.GetRealId(dId))
	if err != nil {
		return false, nil, err
	}

	dupl, err := duplfs.LookupDuplById(*realId)
	if err != nil {
		return false, nil, err
	}

	entityId := realId
	if dupl == nil {
		if util.IsDuplId(dId) {
			status = -10000
		} else {
			ref, err := duplfs.LookupRefById(*realId)
			if err != nil {
				return false, nil, err
			}
			if ref == nil {
				lazyRemove(gridfs, *realId)
				result = true
			} else {
				status = -20000
			}
		}
	} else {
		entityId = &(dupl.Ref)
		err := duplfs.RemoveDupl(dupl.Id)
		if err != nil {
			return false, nil, err
		}

		status, err = duplfs.decAndRemove(gridfs, dupl.Ref)
		if err != nil {
			return false, nil, err
		}
		if status < 0 {
			result = true
		}
	}

	return result, entityId, nil
}

func (duplfs *DuplFs) decAndRemove(gridfs *mgo.GridFS, id bson.ObjectId) (int64, error) {
	ref, err := duplfs.DecRefCnt(id)
	if err == mgo.ErrNotFound {
		duplfs.RemoveRef(id)
		lazyRemove(gridfs, id)
		return -1, nil
	}
	if err != nil {
		return 0, err
	}

	if ref.RefCnt < 0 {
		duplfs.RemoveRef(id)
		lazyRemove(gridfs, id)
	}

	return ref.RefCnt, nil
}

func NewDuplFs(dOp *metadata.DuplicateOp) *DuplFs {
	duplfs := &DuplFs{
		DuplicateOp: dOp,
	}

	return duplfs
}

type FileMeta struct {
	Id          interface{} "_id"
	ChunkSize   int         "chunkSize"
	UploadDate  time.Time   "uploadDate"
	Length      int64       "length,minsize"
	MD5         string      "md5"
	Filename    string      "filename,omitempty"
	ContentType string      "contentType,omitempty"

	Domain int64  "domain"
	UserId string "userid"
	Biz    string "bizname"
}

func LookupFileMeta(gridfs *mgo.GridFS, query bson.D) (*meta.File, error) {
	iter := gridfs.Find(query).Sort("-uploadDate").Iter()
	defer iter.Close()

	fm := new(FileMeta)
	if iter.Next(fm) {
		if oid, ok := fm.Id.(bson.ObjectId); ok {
			return &meta.File{
				Id:         oid.Hex(),
				Biz:        fm.Biz,
				Md5:        fm.MD5,
				Name:       fm.Filename,
				UserId:     fm.UserId,
				ChunkSize:  fm.ChunkSize,
				UploadDate: fm.UploadDate,
				Size:       fm.Length,
				Domain:     fm.Domain,
			}, nil
		}
	}

	return nil, meta.FileNotFound
}

func hexString2ObjectId(hex string) (*bson.ObjectId, error) {
	if bson.IsObjectIdHex(hex) {
		oid := bson.ObjectIdHex(hex)
		return &oid, nil
	}

	return nil, fmt.Errorf("Invalid id, %s", hex)
}
