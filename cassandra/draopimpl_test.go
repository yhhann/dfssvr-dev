package cassandra

import (
	"fmt"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
)

func TestFileWithoutMeta(t *testing.T) {
	if err := testFile(nil); err != nil {
		t.Fatalf("Test file error %v", err)
	}
}

func TestFile(t *testing.T) {
	if err := testFile(getMeta()); err != nil {
		t.Fatalf("Test file error %v", err)
	}
}

func testFile(meta map[string]string) error {
	f := &File{
		Id:         bson.NewObjectId().Hex(),
		Biz:        "test-seadra",
		ChunkSize:  1024,
		Domain:     2,
		Name:       "test-file",
		Size:       2048,
		Md5:        "a3f3694c443da536e401a5a13a8a0836",
		UploadDate: time.Now(),
		UserId:     "1001",
		Type:       EntitySeadraFS,
		Metadata:   meta,
	}

	op := newMetaOp()
	err := op.SaveFile(f)
	if err != nil {
		return fmt.Errorf("save file error %v.", err)
	}

	f1, err := op.LookupFileById(f.Id)
	if err != nil {
		return fmt.Errorf("lookup file by id error %v.", err)
	}

	if f1.Id != f1.Id {
		return fmt.Errorf("lookup file by id error, not the same file.")
	}

	f2, err := op.LookupFileByMd5(f.Md5, f.Domain)
	if err != nil {
		return fmt.Errorf("lookup file by md5 error %v.", err)
	}
	if f2.Id != f1.Id {
		return fmt.Errorf("lookup file by id error, not the same file.")
	}

	for k, v := range meta {
		if f2.Metadata[k] != v {
			return fmt.Errorf("lookup file by id error, not the same metadta.")
		}
	}

	if err = op.RemoveFile(f.Id); err != nil {
		return fmt.Errorf("delete file error %v.", err)
	}

	return nil
}

func TestDupl(t *testing.T) {
	duplId := bson.NewObjectId().Hex()
	refId := bson.NewObjectId().Hex()

	dupl := &Dupl{
		Id:         duplId,
		Ref:        refId,
		Length:     1024,
		CreateDate: time.Now(),
		Domain:     2,
	}

	op := newMetaOp()
	err := op.SaveDupl(dupl)
	if err != nil {
		t.Fatalf("Save dupl error, %v.", err)
	}

	dupl1, err := op.LookupDuplById(duplId)
	if err != nil {
		t.Fatalf("Lookup dupl by id error, %v.", err)
	}
	if dupl1.Id != duplId || dupl1.Ref != refId {
		t.Fatal("Lookup dupl by id error, not the same one.")
	}

	err = op.RemoveDupl(duplId)
	if err != nil {
		t.Fatalf("Remove dupl error, %v.", err)
	}
}

func TestRc(t *testing.T) {
	refId := bson.NewObjectId().Hex()
	ref := &Ref{
		Id:     refId,
		RefCnt: 0,
	}

	op := newMetaOp()
	err := op.SaveRef(ref)
	if err != nil {
		t.Fatalf("Save ref error, %v.", err)
	}

	ref0, err := op.LookupRefById(refId)
	if err != nil {
		t.Fatalf("Lookup ref by id error, %v.", err)
	}
	if ref0.Id != ref.Id || ref0.RefCnt != ref.RefCnt {
		t.Fatal("Lookup ref by id error, not the same one.")
	}

	ref1, err := op.IncRefCnt(refId)
	if err != nil {
		t.Fatalf("Increase ref count error, %v.", err)
	}
	if ref1.RefCnt != 1 {
		t.Fatalf("Increase ref count error, ref cnt %d", ref1.RefCnt)
	}

	ref2, err := op.IncRefCnt(refId)
	if err != nil {
		t.Fatalf("Increase ref count error, %v", err)
	}
	if ref2.RefCnt != 2 {
		t.Fatalf("Increase ref count error, ref cnt %d", ref2.RefCnt)
	}

	ref11, err := op.DecRefCnt(refId)
	if err != nil {
		t.Fatalf("Decrease ref count error, %v", err)
	}
	if ref11.RefCnt != 1 {
		t.Fatalf("Decrease ref count error, ref cnt %d", ref11.RefCnt)
	}

	err = op.RemoveRef(refId)
	if err != nil {
		t.Fatalf("Remove ref error.")
	}
}

func newMetaOp() *DraOpImpl {
	return NewDraOpImpl([]string{"127.0.0.1"})
}

func BenchmarkFile(b *testing.B) {
	var err error
	for i := 0; i < b.N; i++ {
		if err = testFile(getMeta()); err != nil {
			b.Fatalf("Test file error %v", err)
		}
	}
}

func getMeta() map[string]string {
	return map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
}
