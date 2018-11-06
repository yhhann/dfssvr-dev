package sql

import (
	"encoding/json"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"jingoal.com/dfs/meta"
)

var dao FileMetadataDAO

func init() {
	dsn := "root:zcl@tcp(127.0.0.1:3306)/file?charset=utf8"
	_dao, err := NewFMOperator(dsn)
	if err != nil {
		panic(err)
	}
	dao = _dao

}

var fId = "597edb8f4ec50300d28915f7"
var md5 = "a94bd72069b8ced73f94667de235d04f2"
var domain int64 = 110

func newFileMeta() *FileMetadataDO {
	var do = new(FileMetadataDO)
	do.FId = fId
	do.Biz = "TEST"
	do.ChunkSize = 100
	do.Domain = domain
	do.EntityType = meta.EntityGridFS
	do.Md5 = md5
	do.Name = "测试.txt"
	do.RefCount = 1
	do.Size = 1024
	do.UserId = "jingoal"
	do.UploadTime = time.Now()
	return do
}

func TestAddFileMeta(t *testing.T) {
	do := newFileMeta()
	Convey("Add fileMeta ok.", t, func() {
		affectNum, err := dao.AddFileMeta(do)
		So(err, ShouldBeNil)
		So(affectNum, ShouldEqual, 1)

	})

	Convey("Add fileMeta error.", t, func() {
		affectNum, err := dao.AddFileMeta(do)
		So(err, ShouldNotBeNil)
		So(affectNum, ShouldEqual, 0)
	})

}

func TestFindFileMeta(t *testing.T) {
	Convey("Find fileMeta ok.", t, func() {
		_do, err := dao.FindFileMeta(fId)
		So(err, ShouldBeNil)
		So(_do.FId, ShouldEqual, fId)
	})

	Convey("Find fileMeta null.", t, func() {
		_do, err := dao.FindFileMeta("0")
		So(err, ShouldBeNil)
		So(_do, ShouldBeNil)
	})
}

func TestUpdateFileMetaRefCnt(t *testing.T) {
	Convey("Update fileMeta ok.", t, func() {
		affectNum, err := dao.UpdateFileMetaRefCnt(fId, 2)
		So(err, ShouldBeNil)
		So(affectNum, ShouldEqual, 1)

		_do, err := dao.FindFileMeta(fId)
		So(2, ShouldEqual, _do.RefCount)
	})
}

func TestAddExtAttr(t *testing.T) {
	extMap := make(map[string]string)
	extMap["color"] = "green"
	extAttr, _ := json.Marshal(extMap)
	Convey("Add extAttr ok.", t, func() {
		affectNum, err := dao.AddExtAttr(fId, string(extAttr))
		So(err, ShouldBeNil)
		So(affectNum, ShouldEqual, 1)
	})
}

func TestFindExtAttrByFId(t *testing.T) {
	Convey("Find extAttr ok.", t, func() {
		extAttr, err := dao.FindExtAttrByFId(fId)
		So(err, ShouldBeNil)
		So(len(extAttr), ShouldEqual, 1)
	})
}

func TestAddDuplicateInfot(t *testing.T) {
	Convey("Add DuplicationInfo ok.", t, func() {
		affectNum, err := dao.AddDuplicateInfo(fId, "_"+fId, time.Now())
		So(err, ShouldBeNil)
		So(affectNum, ShouldEqual, 1)
	})
}

func TestFindDupInfoByDId(t *testing.T) {
	Convey("Find DuplicationInfo ok.", t, func() {
		_fId, tm, err := dao.FindDupInfoByDId("_" + fId)
		So(err, ShouldBeNil)
		So(tm, ShouldNotBeNil)
		So(_fId, ShouldEqual, fId)
	})
}

func TestFindFileMetaWithExtAttr(t *testing.T) {
	Convey("Find FileMeta with extattr ok.", t, func() {
		fm, err := dao.FindFileMetaWithExtAttr(fId)
		So(err, ShouldBeNil)
		So(fm, ShouldNotBeNil)
		So(fId, ShouldEqual, fId)
		So(fm.ExtAttr, ShouldNotBeNil)
		So(fm.ExtAttr["color"], ShouldEqual, "green")
	})
}

func TestFindFileMetaByMD5WithExtAttr(t *testing.T) {
	Convey("Find FileMeta with extattr by md5 ok.", t, func() {
		fm, err := dao.FindFileMetaByMD5WithExtAttr(md5, domain)
		So(err, ShouldBeNil)
		So(fm, ShouldNotBeNil)
		So(fId, ShouldEqual, fId)
		So(fm.ExtAttr, ShouldNotBeNil)
		So(fm.ExtAttr["color"], ShouldEqual, "green")
	})

}

func TestDeleteDupInfoByDId(t *testing.T) {
	Convey("Delete DupInfo by dId ok.", t, func() {
		num, err := dao.DeleteDupInfoByDId("_" + fId)
		So(err, ShouldBeNil)
		So(num, ShouldEqual, 1)

		_fId, _, err := dao.FindDupInfoByDId("_" + fId)
		So(err, ShouldBeNil)
		So(_fId, ShouldEqual, "")

	})

}

func TestDeleteExtAttr(t *testing.T) {
	Convey("Delete extAttr by dId ok.", t, func() {
		num, err := dao.DeleteExtAttr(fId)
		So(err, ShouldBeNil)
		So(num, ShouldEqual, 1)

		exAttr, err := dao.FindExtAttrByFId(fId)
		So(err, ShouldBeNil)
		So(exAttr, ShouldBeNil)

	})
}

func TestDeleteFileMeta(t *testing.T) {
	Convey("Delete FileMeta by dId ok.", t, func() {
		num, err := dao.DeleteFileMeta(fId)
		So(err, ShouldBeNil)
		So(num, ShouldEqual, 1)

		do, err := dao.FindFileMeta(fId)
		So(err, ShouldBeNil)
		So(do, ShouldBeNil)

	})
}

func TestUpdateFMRefCntIncreOne(t *testing.T) {
	Convey("Update refcnt auto increment 1 ok.", t, func() {
		do := newFileMeta()
		dao.AddFileMeta(do)
		num, err := dao.UpdateFMRefCntIncreOne(fId)
		So(err, ShouldBeNil)
		So(num, ShouldEqual, 1)

		fmdo, err := dao.FindFileMeta(fId)
		So(err, ShouldBeNil)
		So(fmdo.RefCount, ShouldEqual, 2)

		dao.DeleteFileMeta(fId)
	})
}
