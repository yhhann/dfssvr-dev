package sql

import (
	"context"
	"errors"
	"time"

	//	"github.com/golang/glog"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/util"
)

type TiDBMetaImpl struct {
	*DatabaseMgr
}

func NewTiDBMetaImpl(mgr *DatabaseMgr) meta.FileMetaOp {
	return &TiDBMetaImpl{
		DatabaseMgr: mgr,
	}
}

// Save saves the metadata of a file.
func (impl *TiDBMetaImpl) Save(f *meta.File) error {
	if f == nil {
		return InputArgsNull
	}
	if f.Type == meta.EntityNone {
		return errors.New("File type unknown.")
	}

	ctx := context.Background()

	tf := ToTiDBFile(f)
	err := impl.Session(ctx).SaveFile(ctx, tf)
	if err != nil {
		return err
	}

	return nil
}

// Find looks up the metadata of a file by its fid.
func (impl *TiDBMetaImpl) Find(fid string) (*meta.File, error) {
	if fid == "" {
		return nil, InputArgsNull
	}

	ctx := context.Background()

	if util.IsDuplId(fid) {
		tf, err := impl.Session(ctx).LookupFileByDid(ctx, util.GetRealId(fid))
		if err != nil {
			return nil, err
		}

		return ToMetaFile(tf), nil
	}

	tf, err := impl.Session(ctx).LookupFile(ctx, fid)
	if err != nil {
		return nil, err
	}

	return ToMetaFile(tf), nil
}

// FindByMd5 looks up the metadata of a file by its md5 and domain.
func (impl *TiDBMetaImpl) FindByMd5(md5 string, domain int64) (*meta.File, error) {
	if md5 == "" || domain == 0 {
		return nil, InputArgsNull
	}

	ctx := context.Background()

	tf, err := impl.Session(ctx).LookupFileByMD5(ctx, md5, domain)
	if err != nil {
		return nil, err
	}

	return ToMetaFile(tf), nil
}

// DuplicateWithId duplicates a given file by its fid.
func (impl *TiDBMetaImpl) DuplicateWithId(fid string, did string, createDate time.Time) (string, error) {
	if fid == "" {
		return "", InputArgsNull
	}

	if did == "" {
		did = bson.NewObjectId().Hex()
	}

	if time.Time.IsZero(createDate) {
		createDate = time.Now()
	}

	ctx := context.Background()

	if util.IsDuplId(fid) {
		original, err := impl.Session(ctx).LookupFileByDid(ctx, util.GetRealId(fid))
		if err != nil {
			return "", err
		}

		fid = original.FId
	}

	if err := impl.Session(ctx).DuplFile(ctx, fid, util.GetRealId(did), createDate); err != nil {
		return "", err
	}

	return util.GetDuplId(did), nil
}

// Delete deletes a file.
func (impl *TiDBMetaImpl) Delete(fid string) (bool, string, error) {
	if fid == "" {
		return false, "", InputArgsNull
	}

	ctx := context.Background()

	if util.IsDuplId(fid) {
		return impl.Session(ctx).RemoveDupl(ctx, util.GetRealId(fid))
	}

	result, err := impl.Session(ctx).RemoveFile(ctx, fid)
	if err != nil {
		return false, "", err
	}

	return result, fid, nil
}

func ToMetaFile(f *File) *meta.File {
	return &meta.File{
		Id:         f.FId,
		Biz:        f.Biz,
		Name:       f.Name,
		Md5:        f.Md5,
		UserId:     f.UserId,
		Domain:     f.Domain,
		Size:       f.Size,
		ChunkSize:  f.ChunkSize,
		UploadDate: f.UploadTime,
		Type:       meta.EntityType(f.EntityType),
		ExtAttr:    f.ExtAttr,
	}
}

func ToTiDBFile(m *meta.File) *File {
	return &File{
		FId:        m.Id,
		Biz:        m.Biz,
		Name:       m.Name,
		Md5:        m.Md5,
		UserId:     m.UserId,
		Domain:     m.Domain,
		Size:       m.Size,
		ChunkSize:  m.ChunkSize,
		UploadTime: m.UploadDate,
		EntityType: int8(m.Type),
		ExtAttr:    m.ExtAttr,
		RefCount:   1,
	}
}
