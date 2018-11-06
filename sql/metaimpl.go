package sql

import (
	"errors"
	"sync"
	"time"

	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/util"
)

var (
	InputArgsNull     = errors.New("input arguments are null")
	FileNotFound      = errors.New("file not found")
	FileAlreadyExists = errors.New("file already exists")
	mutex             sync.Mutex
)

type MetaImpl struct {
	FileMetadataDAO
}

func NewMetaImpl(fmDao FileMetadataDAO) *MetaImpl {
	return &MetaImpl{
		FileMetadataDAO: fmDao,
	}
}

// Save saves the metadata of a file.
func (mi *MetaImpl) Save(f *meta.File) error {
	if f == nil {
		return InputArgsNull
	}
	if f.Type == meta.EntityNone {
		return errors.New("File type unknown.")
	}

	fm := DraFile(f)
	_, err := mi.AddFileMeta(fm)

	if err != nil {
		return err
	}

	if f.ExtAttr != nil {
		for k, v := range f.ExtAttr {
			singleAttrmap := map[string]string{k: v}
			extAttr := Map2Json(singleAttrmap)
			_, _err := mi.AddExtAttr(f.Id, extAttr)
			err = _err
		}
	}

	return err
}

// Find looks up the metadata of a file by its fid.
func (mi *MetaImpl) Find(fid string) (*meta.File, error) {
	if fid == "" {
		return nil, InputArgsNull
	}

	if util.IsDuplId(fid) {
		ffId, _, err := mi.FindDupInfoByDId(util.GetRealId(fid))
		if err != nil {
			return nil, err
		}
		fid = ffId
	}

	fmdo, err := mi.FindFileMetaWithExtAttr(fid)
	if err != nil || fmdo == nil {
		return nil, err
	}

	fm := MetaFile(fmdo)

	return fm, err
}

// FindByMd5 looks up the metadata of a file by its md5 and domain.
func (mi *MetaImpl) FindByMd5(md5 string, domain int64) (*meta.File, error) {
	if md5 == "" || domain == 0 {
		return nil, InputArgsNull
	}

	fmdo, err := mi.FindFileMetaByMD5WithExtAttr(md5, domain)

	if err != nil || fmdo == nil {
		return nil, err
	}

	fm := MetaFile(fmdo)

	return fm, err
}

// DuplicateWithId duplicates a given file by its fid.
func (mi *MetaImpl) DuplicateWithId(fid string, did string, createDate time.Time) (string, error) {
	if fid == "" {
		return "", InputArgsNull
	}

	if did == "" {
		did = bson.NewObjectId().Hex()
	}
	if time.Time.IsZero(createDate) {
		createDate = time.Now()
	}

	if util.IsDuplId(fid) {
		orignalFid, _, err := mi.FindDupInfoByDId(util.GetRealId(fid))
		if err != nil {
			return "", err
		}
		fid = orignalFid
	}

	do, _ := mi.FindFileMeta(fid)
	if do == nil {
		return "", FileNotFound
	}

	_, err := mi.AddDuplicateInfo(fid, did, createDate)
	if err != nil {
		return "", err
	}

	_, erru := mi.UpdateFMRefCntIncreOne(fid)
	if erru != nil {
		return "", erru
	}

	return util.GetDuplId(did), err
}

// Delete deletes a file.
func (mi *MetaImpl) Delete(fid string) (tobeDeleted bool, entityIdToBeDeleted string, err error) {
	if fid == "" {
		return false, "", InputArgsNull
	}
	var dId string
	if util.IsDuplId(fid) {
		dId = fid
		ffId, _, err := mi.FindDupInfoByDId(util.GetRealId(dId))
		if err != nil {
			return false, "", err
		}
		fid = ffId
	}

	// start,maybe can optimize
	mutex.Lock()
	defer mutex.Unlock()
	fmdo, err := mi.FindFileMeta(fid)
	if err != nil {
		return false, "", err
	}

	nRefCnt := fmdo.RefCount - 1
	if nRefCnt > 0 {
		_, err := mi.UpdateFileMetaRefCnt(fid, nRefCnt)
		if err != nil {
			return false, "", err
		}

		if util.IsDuplId(dId) {
			_, errd := mi.DeleteDupInfoByDId(util.GetRealId(dId))
			if errd != nil {
				return false, "", errd
			}
		}
		return false, "", nil
	} else {
		_, err := mi.DeleteExtAttr(fid)
		if err != nil {
			return false, "", err
		}

		_, errd := mi.DeleteDupInfoByFid(fid)
		if errd != nil {
			return false, "", errd
		}
		_, errdf := mi.DeleteFileMeta(fid)
		if errdf != nil {
			return false, "", errdf
		}
	}
	//end

	return true, fid, nil
}
