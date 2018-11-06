package sql

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	file_field      = "id, name, biz, md5, uid, domain, size, chunksize, entitytype, refcnt, uploadtime"
	f_insert        = "INSERT INTO file (" + file_field + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	f_select        = "SELECT " + file_field + " FROM file WHERE id = ?"
	f_sel_by_md5    = "SELECT " + file_field + " FROM file WHERE md5 = ? AND domain = ? ORDER BY uploadtime DESC"
	f_sel_by_did    = "SELECT " + file_field + " FROM file as f JOIN dupl as d ON (f.id = d.fid) WHERE d.did = ?"
	f_update_refcnt = "UPDATE file set refcnt = ? WHERE id=?"
	f_ref_inc_one   = "UPDATE file SET refcnt=refcnt+1 WHERE id =?"
	f_ref_dec_one   = "UPDATE file SET refcnt=refcnt-1 WHERE id =?"
	f_delete        = "DELETE FROM file WHERE id=?"
	a_insert        = "INSERT INTO attr (fid, k, v) VALUES (?, ?, ?)"
	a_select        = "SELECT k, v from attr WHERE fid = ?"
	a_delete        = "DELETE FROM attr WHERE fid=?"
	d_insert        = "INSERT INTO dupl (did, fid, createdtime) VALUES (?, ?, ?)"
	d_select        = "SELECT fid, createdtime from dupl WHERE did = ?"
	d_delete_bydid  = "DELETE FROM dupl WHERE did=?"
	d_delete_byfid  = "DELETE FROM dupl WHERE fid=?"
)

var (
	FileNotFoundError = errors.New("file not found")
	ParameterError    = errors.New("parameter error")
)

type File struct {
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
	EntityType int8
}

// FMoperator implements interface FileMetadataDAO.
type FOperator struct {
	db *sql.DB
}

// LookupFile returns a file metadata and attributes by it's fid.
func (operator FOperator) LookupFile(ctx context.Context, fid string) (*File, error) {
	var f *File
	err := operator.Tx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(f_select)
		if err != nil {
			return err
		}
		defer stmt.Close()

		fm := &File{}
		var upload_time string

		err = stmt.QueryRow(fid).Scan(&fm.FId, &fm.Name, &fm.Biz, &fm.Md5, &fm.UserId, &fm.Domain, &fm.Size, &fm.ChunkSize, &fm.EntityType, &fm.RefCount, &upload_time)
		if err != nil {
			return err
		}

		timeLayout := "2006-01-02 15:04:05"
		fm.UploadTime, err = time.Parse(timeLayout, upload_time)

		attrs, err := operator.getFileAttrs(ctx, tx, fid)
		if err != nil {
			return err
		}
		fm.ExtAttr = attrs

		f = fm
		return nil
	})

	return f, err
}

// LookupFileByDid returns a file metadata and attributes by duplicate id.
func (operator FOperator) LookupFileByDid(ctx context.Context, did string) (*File, error) {
	var f *File
	err := operator.Tx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(f_sel_by_did)
		if err != nil {
			return err
		}
		defer stmt.Close()

		fm := &File{}
		var upload_time string

		err = stmt.QueryRow(did).Scan(&fm.FId, &fm.Name, &fm.Biz, &fm.Md5, &fm.UserId, &fm.Domain, &fm.Size, &fm.ChunkSize, &fm.EntityType, &fm.RefCount, &upload_time)
		if err != nil {
			return err
		}

		timeLayout := "2006-01-02 15:04:05"
		fm.UploadTime, err = time.Parse(timeLayout, upload_time)

		attrs, err := operator.getFileAttrs(ctx, tx, fm.FId)
		if err != nil {
			return err
		}

		fm.ExtAttr = attrs

		f = fm
		return nil
	})

	return f, err
}

// LookupFileByMD5 returns file metadata without attributes by it's md5.
func (operator FOperator) LookupFileByMD5(ctx context.Context, md5 string, domain int64) (*File, error) {
	var f *File
	err := operator.Tx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(f_sel_by_md5)
		if err != nil {
			return err
		}
		defer stmt.Close()

		fm := &File{}
		var upload_time string

		err = stmt.QueryRow(md5, domain).Scan(&fm.FId, &fm.Name, &fm.Biz, &fm.Md5, &fm.UserId, &fm.Domain, &fm.Size, &fm.ChunkSize, &fm.EntityType, &fm.RefCount, &upload_time)
		if err != nil {
			return err
		}

		timeLayout := "2006-01-02 15:04:05"
		fm.UploadTime, err = time.Parse(timeLayout, upload_time)

		f = fm
		return nil
	})
	return f, err
}

// SaveFile saves file metadata and it's attributes if any.
func (operator FOperator) SaveFile(ctx context.Context, fm *File) error {
	return operator.Tx(func(tx *sql.Tx) error {
		err := operator.saveFile(ctx, tx, fm)
		if err != nil {
			return err
		}

		if len(fm.ExtAttr) == 0 {
			return nil
		}

		return operator.saveAttrs(ctx, tx, fm.FId, fm.ExtAttr)
	})
}

// DuplFile returns a reference for an entity file.
func (operator FOperator) DuplFile(ctx context.Context, fid string, did string, createDate time.Time) error {
	return operator.Tx(func(tx *sql.Tx) error {
		if err := operator.saveDupl(ctx, tx, fid, did, createDate); err != nil {
			return err
		}

		if err := operator.refInc(ctx, tx, fid); err != nil {
			return err
		}

		return nil
	})
}

// AddAttrs adds attributes for a file.
func (operator FOperator) AddAttrs(ctx context.Context, fid string, attrs map[string]string) error {
	return operator.Tx(func(tx *sql.Tx) error {
		return operator.saveAttrs(ctx, tx, fid, attrs)
	})
}

// RemoveFile removes a file with attributes if any.
func (operator FOperator) RemoveFile(ctx context.Context, fid string) (bool, error) {
	result := false
	err := operator.Tx(func(tx *sql.Tx) error {
		var err error
		result, err = operator.removeFile(ctx, tx, fid)
		return err
	})

	return result, err
}

// RemoveDupl removes the duplication, decrease it's ref,
// if ref == 0, then remove the entity file and return true.
func (operator FOperator) RemoveDupl(ctx context.Context, did string) (bool, error) {
	result := false

	f, err := operator.LookupFileByDid(ctx, did)
	if err != nil {
		return false, err
	}

	err = operator.Tx(func(tx *sql.Tx) error {
		n, err := operator.removeDupl(ctx, tx, did)
		if err != nil {
			return err
		}

		if n == 0 {
			return nil
		}

		result, err = operator.removeFile(ctx, tx, f.FId)
		return err
	})

	return result, err
}

func (operator FOperator) saveFile(ctx context.Context, tx *sql.Tx, fm *File) error {
	if fm.FId == "" {
		return ParameterError
	}
	fm.RefCount = 1

	stmt, err := tx.Prepare(f_insert)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(fm.FId, fm.Name, fm.Biz, fm.Md5, fm.UserId, fm.Domain, fm.Size, fm.ChunkSize, fm.EntityType, fm.RefCount, fm.UploadTime)
	if err != nil {
		return err
	}

	return nil
}

// saveAttrs saves attributes of file.
func (operator FOperator) saveAttrs(ctx context.Context, tx *sql.Tx, fid string, attrs map[string]string) error {
	if len(attrs) == 0 {
		return nil
	}

	stmt, err := tx.Prepare(a_insert)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for k, v := range attrs {
		_, err := stmt.Exec(fid, k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (operator FOperator) saveDupl(ctx context.Context, tx *sql.Tx, fid string, did string, createDate time.Time) error {
	stmt, err := tx.Prepare(d_insert)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(did, fid, createDate)
	if err != nil {
		return err
	}

	return nil
}

func (operator FOperator) refInc(ctx context.Context, tx *sql.Tx, fid string) error {
	stmt1, err := tx.Prepare(f_ref_inc_one)
	if err != nil {
		return err
	}
	defer stmt1.Close()

	_, err = stmt1.Exec(fid)
	if err != nil {
		return err
	}

	return nil
}

func (operator FOperator) refDec(ctx context.Context, tx *sql.Tx, fid string) error {
	stmt1, err := tx.Prepare(f_ref_dec_one)
	if err != nil {
		return err
	}
	defer stmt1.Close()

	_, err = stmt1.Exec(fid)
	if err != nil {
		return err
	}

	return nil
}

func (operator FOperator) getRef(ctx context.Context, tx *sql.Tx, fid string) (int, error) {
	stmt1, err := tx.Prepare("SELECT refcnt FROM file where id = ?")
	if err != nil {
		return -1, err
	}
	defer stmt1.Close()

	var refcnt int
	err = stmt1.QueryRow(fid).Scan(&refcnt)
	if err != nil {
		return -1, err
	}

	return refcnt, nil
}

func (operator FOperator) removeFileAndAttrs(ctx context.Context, tx *sql.Tx, fid string) error {
	stmt, err := tx.Prepare(f_delete)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(fid)
	if err != nil {
		return err
	}

	stmt1, err := tx.Prepare(a_delete)
	if err != nil {
		return err
	}
	defer stmt1.Close()

	_, err = stmt1.Exec(fid)
	if err != nil {
		return err
	}

	return nil
}

func (operator FOperator) removeDupl(ctx context.Context, tx *sql.Tx, did string) (int64, error) {
	stmt, err := tx.Prepare(d_delete_bydid)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(did)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (operator FOperator) removeFile(ctx context.Context, tx *sql.Tx, fid string) (bool, error) {
	if err := operator.refDec(ctx, tx, fid); err != nil {
		return false, err
	}

	refcnt, err := operator.getRef(ctx, tx, fid)
	if err != nil {
		return false, err
	}

	if refcnt > 0 {
		return false, nil
	}

	if err = operator.removeFileAndAttrs(ctx, tx, fid); err != nil {
		return false, err
	}

	return true, nil
}

func (operator FOperator) getFileAttrs(ctx context.Context, tx *sql.Tx, fid string) (map[string]string, error) {
	attrs := make(map[string]string)

	stmt1, err := tx.Prepare(a_select)
	if err != nil {
		return attrs, err
	}
	defer stmt1.Close()

	rows, err := stmt1.Query(fid)
	if err != nil {
		return attrs, err
	}
	defer rows.Close()

	for rows.Next() {
		var k, v string
		err = rows.Scan(&k, &v)
		if err != nil {
			return attrs, err
		}

		attrs[k] = v
	}

	return attrs, nil
}

func (operator FOperator) Tx(target func(*sql.Tx) error) error {
	tx, err := operator.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	err = target(tx)

	return err
}

func NewFOperator(db *sql.DB) (*FOperator, error) {
	return &FOperator{
		db: db,
	}, nil
}
