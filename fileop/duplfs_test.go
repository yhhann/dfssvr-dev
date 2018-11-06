package fileop

import (
	"testing"
	"time"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/util"
)

var (
	dbUri  = "mongodb://192.168.55.193:27017/"
	dbName = "gridfs1"
)

func prepareDuplFs() (*DuplFs, error) {
	session, err := metadata.OpenMongoSession(dbUri)
	if err != nil {
		return nil, err
	}
	gridfs := session.Copy().DB(dbName).GridFS("fs")

	dup, err := metadata.NewDuplicateOp(session, dbName, "fs")
	if err != nil {
		return nil, err
	}

	return NewDuplFs(gridfs, dup), nil
}

func createGridFsFile(name string) (interface{}, error) {
	session, err := metadata.OpenMongoSession(dbUri)
	if err != nil {
		return nil, err
	}
	gridfs := session.Copy().DB(dbName).GridFS("fs")

	gFile, err := gridfs.Create(name)
	if err != nil {
		return nil, err
	}

	gFile.Write([]byte("this is a test data."))
	if err := gFile.Close(); err != nil {
		return nil, err
	}

	return gFile.Id(), nil
}

func TestDuplicate(t *testing.T) {
	duplfs, err := prepareDuplFs()
	if err != nil {
		t.Errorf("prepare DuplFs error: %v", err)
	}

	origin, err := createGridFsFile("test-file")
	if err != nil {
		t.Errorf("create gridfs file error: %v", err)
	}

	originFid, ok := origin.(bson.ObjectId)
	if !ok {
		t.Errorf("file id is not ObjectId, %T", origin)
	}

	// Find
	gFile, err := duplfs.Find(originFid.Hex())
	if err != nil {
		t.Errorf("find file error: %v", err)
	}
	defer gFile.Close()

	dupId := bson.NewObjectId()
	uploadDate := time.Now()

	dId, err := duplfs.DuplicateWithId(originFid.Hex() /* oid */, dupId.Hex(), uploadDate)
	if err != nil {
		t.Errorf("DuplicateWithId error: %v", err)
	}

	// Find dupl
	gFile2, err := duplfs.Find(originFid.Hex())
	if err != nil {
		t.Errorf("find file error: %v", err)
	}
	defer gFile2.Close()

	if gFile.Id() != gFile2.Id() {
		t.Errorf("file found not the same.")
	}

	if gFile.MD5() != gFile2.MD5() {
		t.Errorf("file found not the same.")
	}

	if !util.IsDuplId(dId) {
		t.Errorf("duplicate error: not a duplicate id.")
	}

	if util.GetRealId(dId) != dupId.Hex() {
		t.Errorf("return id is not perfered id")
	}

	result, err := duplfs.Delete(dId)
	if err != nil {
		t.Errorf("delete dup error: %v", err)
	}

	if result {
		t.Errorf("delete dup error: can not delete real file.")
	}

	_, err = duplfs.Find(dId)
	if err == nil {
		t.Errorf("find file error: file must be lost.")
	}

	gFile4, err := duplfs.Find(originFid.Hex())
	if err != nil {
		t.Errorf("find file error: %v", err)
	}
	defer gFile4.Close()

	result, err = duplfs.Delete(originFid.Hex())
	if err != nil {
		t.Errorf("delete real file error: %v", err)
	}

	if !result {
		t.Errorf("delete real file error: not delete real file.")
	}

	_, err = duplfs.Find(originFid.Hex())
	if err == nil {
		t.Errorf("find file error: file must be lost.")
	}
}

func TestFindByMd5(t *testing.T) {
	duplfs, err := prepareDuplFs()
	if err != nil {
		t.Errorf("prepare DuplFs error: %v", err)
	}

	f, err := duplfs.FindByMd5("08c93d421c5c71fc38be66825f77d700", 5, 1858)
	if err != nil {
		t.Errorf("Find by md5 error: %v", err)
	}

	query := bson.D{
		{"_id", f.Id()},
	}
	fm, err := LookupFileMeta(duplfs.gridfs, query)
	if err != nil {
		t.Errorf("Lookup file metadata error: %v", err)
	}

	glog.Infof("%d, %s, %d, %s, %s", fm.Length, fm.MD5, fm.Domain, fm.UserId, fm.Biz)
}
