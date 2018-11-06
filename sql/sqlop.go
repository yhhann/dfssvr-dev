package sql

import (
	"time"
)

type FileMetadataDAO interface {

	// 添加表file_metadata记录
	AddFileMeta(fm *FileMetadataDO) (int, error)

	// 删除表file_metadata记录
	DeleteFileMeta(fId string) (int, error)

	// 更新表file_metadata字段ref_cnt
	UpdateFileMetaRefCnt(fId string, refCount int) (int, error)

	// 查询表file_metadata记录
	FindFileMeta(fId string) (*FileMetadataDO, error)

	// 根据fId查询表file_metadata和ext_attr，返回带有extAttr属性的对象
	FindFileMetaWithExtAttr(fId string) (*FileMetadataDO, error)

	// 根据md5,domain查询表file_metadata和ext_attr，返回带有extAttr属性的对象
	FindFileMetaByMD5WithExtAttr(md5 string, domain int64) (*FileMetadataDO, error)

	// 添加表ext_attr记录
	AddExtAttr(fId string, extAttr string) (int, error)

	// 根据fId删除表ext_attr记录
	DeleteExtAttr(fId string) (int, error)

	// 根据fId查询表ext_attr记录
	FindExtAttrByFId(fId string) ([]string, error)

	// 添加表duplicate_info记录
	AddDuplicateInfo(fId string, dId string, createDate time.Time) (int, error)

	// 根据dId删除表duplicate_info记录
	DeleteDupInfoByDId(dId string) (int, error)

	// 根据fId删除表duplicate_info记录
	DeleteDupInfoByFid(fId string) (int, error)

	// 根据dId查询表duplicate_info记录
	FindDupInfoByDId(dId string) (string, time.Time, error)

	// 更新表file_metadata字段ref_cnt,值自动+1
	UpdateFMRefCntIncreOne(fId string) (int, error)
}
