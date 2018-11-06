package sql

import (
	"database/sql"
	"encoding/json"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"jingoal.com/dfs/meta"
)

const (
	fm_insert           = "INSERT INTO file_metadata (id,name,biz,md5,user_id,domain,size,chun_size,entity_type,ref_cnt,upload_time) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
	fm_delete           = "DELETE FROM file_metadata WHERE id=?"
	fm_update_refcnt    = "UPDATE file_metadata set ref_cnt = ? WHERE id=?"
	fm_select           = "SELECT id,name,biz,md5,user_id,domain,size,chun_size,entity_type,ref_cnt,upload_time FROM file_metadata WHERE id = ?"
	ea_insert           = "INSERT INTO ext_attr (fId,attr) VALUES (?,?)"
	ea_delete           = "DELETE FROM ext_attr WHERE fId=?"
	ea_select           = "SELECT attr from ext_attr WHERE fId = ?"
	di_insert           = "INSERT INTO duplicate_info (dId,fId,created_time) VALUES (?,?,?)"
	di_delete_bydid     = "DELETE FROM duplicate_info WHERE dId=?"
	di_delete_byfid     = "DELETE FROM duplicate_info WHERE fId=?"
	di_select           = "SELECT fId,created_time from duplicate_info WHERE dId = ?"
	fm_sel_bydomainmd5  = "SELECT id,name,biz,user_id,size,chun_size,entity_type,ref_cnt,upload_time FROM file_metadata WHERE domain = ? AND md5 = ? ORDER BY upload_time DESC"
	fm_sel_withextattr  = "SELECT id,name,biz,user_id,domain,size,chun_size,entity_type,ref_cnt,upload_time FROM file_metadata WHERE id = ? "
	fm_refcnt_incre_one = "UPDATE file_metadata SET ref_cnt=ref_cnt+1 WHERE id =?"
)

type FileMetadataDO struct {
	FId        string
	Name       string
	Biz        string
	Md5        string
	UserId     string
	ExtAttr    map[string]string
	Domain     int64
	Size       int64
	ChunkSize  int
	RefCount   int
	UploadTime time.Time
	EntityType meta.EntityType
}

// FMoperator implements interface FileMetadataDAO.
type FMOperator struct {
	db *sql.DB
}

func (operator FMOperator) AddFileMeta(fm *FileMetadataDO) (int, error) {

	stmt, err := operator.db.Prepare(fm_insert)
	if err != nil {
		return 0, err
	}

	defer stmt.Close()
	res, err := stmt.Exec(fm.FId, fm.Name, fm.Biz, fm.Md5, fm.UserId, fm.Domain, fm.Size, fm.ChunkSize, fm.EntityType, fm.RefCount, fm.UploadTime)
	if err != nil {
		return 0, err
	}
	id, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(id), err
}

func (operator FMOperator) DeleteFileMeta(fId string) (int, error) {

	stmt, err := operator.db.Prepare(fm_delete)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(fId)
	if err != nil {
		return 0, err
	}
	num, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(num), err
}

func (operator FMOperator) UpdateFileMetaRefCnt(fId string, refCount int) (int, error) {
	stmt, err := operator.db.Prepare(fm_update_refcnt)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(refCount, fId)
	if err != nil {
		return 0, err
	}
	num, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(num), err
}

func (operator FMOperator) FindFileMeta(fId string) (*FileMetadataDO, error) {
	stmt, err := operator.db.Prepare(fm_select)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.Query(fId)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	var fm = new(FileMetadataDO)
	var upload_time string

	err = rows.Scan(&fm.FId, &fm.Name, &fm.Biz, &fm.Md5, &fm.UserId, &fm.Domain, &fm.Size, &fm.ChunkSize, &fm.EntityType, &fm.RefCount, &upload_time)
	if err != nil {
		return nil, err
	}

	timeLayout := "2006-01-02 15:04:05"
	fm.UploadTime, err = time.Parse(timeLayout, upload_time)

	return fm, err
}

func (operator FMOperator) AddExtAttr(fId string, extAttr string) (int, error) {
	stmt, err := operator.db.Prepare(ea_insert)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(fId, extAttr)
	if err != nil {
		return 0, err
	}
	id, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(id), err
}

func (operator FMOperator) DeleteExtAttr(fId string) (int, error) {
	stmt, err := operator.db.Prepare(ea_delete)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(fId)
	if err != nil {
		return 0, err
	}
	num, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(num), err
}

func (operator FMOperator) FindExtAttrByFId(fId string) ([]string, error) {
	stmt, err := operator.db.Prepare(ea_select)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.Query(fId)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	defer rows.Close()
	var attrs []string
	i := 0
	for rows.Next() {
		attrs = append(attrs, "")
		err = rows.Scan(&attrs[i])
		i++
	}

	return attrs, err
}

func (operator FMOperator) AddDuplicateInfo(fId string, dId string, createDate time.Time) (int, error) {
	stmt, err := operator.db.Prepare(di_insert)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(dId, fId, createDate)
	if err != nil {
		return 0, err
	}
	id, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(id), err
}

func (operator FMOperator) DeleteDupInfoByDId(dId string) (int, error) {
	stmt, err := operator.db.Prepare(di_delete_bydid)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(dId)
	if err != nil {
		return 0, err
	}
	num, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(num), err
}

func (operator FMOperator) DeleteDupInfoByFid(fId string) (int, error) {
	stmt, err := operator.db.Prepare(di_delete_byfid)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(fId)
	if err != nil {
		return 0, err
	}
	num, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(num), err
}

func (operator FMOperator) FindDupInfoByDId(dId string) (string, time.Time, error) {
	stmt, err := operator.db.Prepare(di_select)
	if err != nil {
		return "", time.Now(), err
	}
	defer stmt.Close()

	rows, err := stmt.Query(dId)

	if err != nil {
		return "", time.Now(), err
	}
	defer rows.Close()

	if !rows.Next() {
		return "", time.Now(), err
	}

	var ffId, fcreatedTime string
	_err := rows.Scan(&ffId, &fcreatedTime)
	if _err != nil {
		return "", time.Now(), err
	}

	timeLayout := "2006-01-02 15:04:05"
	createdT, err := time.Parse(timeLayout, fcreatedTime)

	return ffId, createdT, err
}

func (operator FMOperator) FindFileMetaByMD5WithExtAttr(md5 string, domain int64) (*FileMetadataDO, error) {
	stmt, err := operator.db.Prepare(fm_sel_bydomainmd5)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(domain, md5)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fm = new(FileMetadataDO)
	var upload_time string

	if !rows.Next() {
		return nil, nil
	}

	err = rows.Scan(&fm.FId, &fm.Name, &fm.Biz, &fm.UserId, &fm.Size, &fm.ChunkSize, &fm.EntityType, &fm.RefCount, &upload_time)

	if err != nil {
		return nil, err
	}

	timeLayout := "2006-01-02 15:04:05"
	fm.UploadTime, err = time.Parse(timeLayout, upload_time)

	attrs, err := operator.FindExtAttrByFId(fm.FId)
	if err != nil || attrs == nil {
		return nil, err
	}

	extAttrMap := make(map[string]string)
	for _, value := range attrs {
		json.Unmarshal([]byte(value), &extAttrMap)
	}

	fm.ExtAttr = extAttrMap

	return fm, err
}

func (operator FMOperator) FindFileMetaWithExtAttr(fId string) (*FileMetadataDO, error) {
	fm, err := operator.FindFileMeta(fId)
	if err != nil || fm == nil {
		return nil, err
	}

	attrs, err := operator.FindExtAttrByFId(fm.FId)
	if err != nil || attrs == nil {
		return nil, err
	}
	extAttrMap := make(map[string]string)
	for _, value := range attrs {
		json.Unmarshal([]byte(value), &extAttrMap)
	}

	fm.ExtAttr = extAttrMap
	return fm, err
}

func (operator FMOperator) UpdateFMRefCntIncreOne(fId string) (int, error) {
	stmt, err := operator.db.Prepare(fm_refcnt_incre_one)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(fId)
	if err != nil {
		return 0, err
	}
	num, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(num), err
}

func NewFMOperator(dsn string) (*FMOperator, error) {
	fmOperator := new(FMOperator)
	_db, err := sql.Open("mysql", dsn)
	fmOperator.db = _db
	return fmOperator, err
}
