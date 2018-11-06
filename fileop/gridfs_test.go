package fileop

import (
	"testing"
	"time"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
)

func TestServerStatus(t *testing.T) {
	session, err := metadata.OpenMongoSession(dbUri)
	if err != nil {
		t.Errorf("Open mongo session error: %v", err)
	}

	defer session.Close()

	glog.Infof("waiting for shutdown server!")
	time.Sleep(10 * time.Second) // try to shutdown the mongodb server.
	glog.Infof("wait over!")

	if !Ok(session) {
		t.Error("status is not ok")
	}
}

func Ok(session *mgo.Session) bool {
	err := session.Run("serverStatus", nil)
	return err == nil
}

func TestGridFs(t *testing.T) {
	shard := &metadata.Shard{
		Name:        "gluster-test",
		Uri:         "mongodb://192.168.55.193:27017/",
		PathVersion: 3,
		PathDigit:   2,
		VolHost:     "192.168.55.193",
		VolName:     "vol2",
		VolBase:     "base-test",
	}
	// Initialize
	handler, err := NewGridFsHandler(shard)
	if err != nil {
		t.Errorf("NewGlusterHandler error %v", err)
		return
	}

	// File info
	info := transfer.FileInfo{Name: "mytestfile",
		Domain: 2,
		User:   101,
	}

	// Create file
	file, err := handler.Create(&info)
	if err != nil {
		t.Errorf("Create file error %v", err)
	}

	p, degist := makePayload(2049)
	wl, err := file.Write(p)
	if err != nil {
		t.Errorf("Write error %v\n", err)
	}
	if wl != 2049 {
		t.Errorf("Write error %v\n", err)
	}

	f, ok := file.(*GridFsFile)
	if !ok {
		t.Errorf("create file type is not grid file\n")
	}

	fid := f.Id()
	fids, ok := fid.(bson.ObjectId)
	if !ok {
		t.Errorf("fid is not ObjectId, %T", fid)
	}

	if err := file.Close(); err != nil {
		t.Errorf("Close write file error %v\n", err)
	}

	// Open file
	nf, err := handler.Open(fids.Hex(), 2)
	if err != nil {
		t.Errorf("Open file error %v\n", err)
	}

	nf1, ok := nf.(*GridFsFile)
	if !ok {
		t.Errorf("open file type is not gluster file\n")
	}

	// Compare md5
	if nf1.info.Md5 != degist {
		t.Errorf("write read file, md5 not equals\n")
	}

	if err := nf.Close(); err != nil {
		t.Errorf("Close read file error %v\n", err)
	}

	if _, err := handler.Remove(fids.Hex(), 2); err != nil {
		t.Errorf("Remove file error %v\n", err)
	}
}
