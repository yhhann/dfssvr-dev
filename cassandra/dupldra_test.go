package cassandra

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/util"
)

var (
	FileAlreadyExists = errors.New("file already exists")
)

var (
	metaFileOk = &meta.File{
		Id:     "564edc6965d7caed29056b5b",
		Md5:    "592f4b95dc96a290e3d80233ffc08b50",
		Domain: 2,
	}
	draFileOk = &File{
		Id:     "564edc6965d7caed29056b5b",
		Md5:    "592f4b95dc96a290e3d80233ffc08b50",
		Domain: 2,
	}

	metaFileErr = &meta.File{
		Id: "56a04788964e5650259db848",
	}
	draFileErr = &File{
		Id: "56a04788964e5650259db848",
	}

	dupl = &Dupl{
		Id:         "59804792bccf53638c57327b",
		Ref:        "564edc6965d7caed29056b5b",
		Length:     1073,
		CreateDate: time.Now(),
		Domain:     2,
	}
	ref = &Ref{
		Id:     "564edc6965d7caed29056b5b",
		RefCnt: 0,
	}

	duplId = "5980479fbccf53638c573902"
)

func TestDraSave(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mockDraOp := NewMockDraOp(ctl)
	duplDra := NewDuplDra(mockDraOp)

	mockDraOp.EXPECT().SaveFile(gomock.Eq(draFileOk)).Return(nil)
	mockDraOp.EXPECT().SaveFile(gomock.Eq(draFileErr)).Return(FileAlreadyExists)

	Convey("Save file ok.", t, func() {
		err := duplDra.Save(metaFileOk)
		So(err, ShouldBeNil)
	})

	Convey("Save file error.", t, func() {
		err := duplDra.Save(metaFileErr)
		So(err, ShouldEqual, FileAlreadyExists)
	})
}

func TestDraFind(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mockDraOp := NewMockDraOp(ctl)
	duplDra := NewDuplDra(mockDraOp)

	mockDraOp.EXPECT().LookupFileById(draFileOk.Id).Return(draFileOk, nil)
	Convey("Find a file.", t, func() {
		f, err := duplDra.Find(draFileOk.Id)
		So(err, ShouldBeNil)
		So(f.Id, ShouldEqual, metaFileOk.Id)
	})
}

func TestDraFindByMd5(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mockDraOp := NewMockDraOp(ctl)
	duplDra := NewDuplDra(mockDraOp)

	mockDraOp.EXPECT().LookupFileByMd5(draFileOk.Md5, draFileOk.Domain).Return(draFileOk, nil)

	Convey("Find a file by its md5.", t, func() {
		f, err := duplDra.FindByMd5(draFileOk.Md5, draFileOk.Domain)
		So(err, ShouldBeNil)
		So(f.Id, ShouldEqual, metaFileOk.Id)
	})
}

func TestDraDuplicateWithIdFirst(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mockDraOp := NewMockDraOp(ctl)
	duplDra := NewDuplDra(mockDraOp)

	mockDraOp.EXPECT().LookupFileById(gomock.Eq(draFileOk.Id)).Return(draFileOk, nil)
	mockDraOp.EXPECT().LookupRefById(ref.Id).Return(nil, nil)
	mockDraOp.EXPECT().SaveRef(ref).Return(nil)
	mockDraOp.EXPECT().SaveDupl(gomock.Any()).Do(func(dupl *Dupl) {
		if dupl.CreateDate.IsZero() {
			dupl.CreateDate = time.Now()
		}
	}).Return(nil)
	mockDraOp.EXPECT().IncRefCnt(ref.Id).Do(func(key string) { ref.RefCnt++ }).Return(ref, nil)
	mockDraOp.EXPECT().SaveDupl(gomock.Any()).Do(func(dupl *Dupl) {
		if len(dupl.Id) == 0 {
			dupl.Id = duplId
		}
		if dupl.CreateDate.IsZero() {
			dupl.CreateDate = time.Now()
		}
	}).Return(nil)

	Convey("Duplicate file.", t, func() {
		did, err := duplDra.DuplicateWithId(draFileOk.Id, "", time.Time{})
		So(err, ShouldBeNil)
		So(did, ShouldEqual, util.GetDuplId(duplId))
	})
}

func TestDraDuplicateWithIdSecond(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mockDraOp := NewMockDraOp(ctl)
	duplDra := NewDuplDra(mockDraOp)

	rDupl := *dupl
	rDupl.Id = duplId

	mockDraOp.EXPECT().LookupDuplById(duplId).Return(&rDupl, nil)
	mockDraOp.EXPECT().LookupFileById(draFileOk.Id).Return(draFileOk, nil)
	mockDraOp.EXPECT().LookupRefById(ref.Id).Return(ref, nil)
	mockDraOp.EXPECT().IncRefCnt(ref.Id).Do(func(key string) { ref.RefCnt++ }).Return(ref, nil)
	mockDraOp.EXPECT().SaveDupl(gomock.Any()).Do(func(dupl *Dupl) {
		if len(dupl.Id) == 0 {
			dupl.Id = duplId
		}
		if dupl.CreateDate.IsZero() {
			dupl.CreateDate = time.Now()
		}
	}).Return(nil)

	Convey("Duplicate file from a duplicated ID.", t, func() {
		did, err := duplDra.DuplicateWithId(util.GetDuplId(duplId), "", time.Time{})
		So(err, ShouldBeNil)
		So(did, ShouldEqual, util.GetDuplId(duplId))
	})
}

func TestDraDeleteFile(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mockDraOp := NewMockDraOp(ctl)
	duplDra := NewDuplDra(mockDraOp)

	mockDraOp.EXPECT().LookupDuplById(gomock.Any()).Return(nil, nil)
	mockDraOp.EXPECT().LookupRefById(gomock.Any()).Return(nil, nil)

	Convey("Delete file", t, func() {
		ok, eid, err := duplDra.Delete(draFileOk.Id)
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)
		So(eid, ShouldEqual, draFileOk.Id)
	})
}

func TestDraDeleteDupl(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mockDraOp := NewMockDraOp(ctl)
	duplDra := NewDuplDra(mockDraOp)

	rref := *ref

	mockDraOp.EXPECT().LookupDuplById(dupl.Id).Return(dupl, nil)
	mockDraOp.EXPECT().RemoveDupl(dupl.Id).Return(nil)
	mockDraOp.EXPECT().DecRefCnt(dupl.Ref).Do(func(id string) { rref.RefCnt = -1 }).Return(&rref, nil)
	mockDraOp.EXPECT().RemoveRef(dupl.Ref)

	Convey("Delete file", t, func() {
		ok, eid, err := duplDra.Delete(dupl.Id)
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)
		So(eid, ShouldEqual, draFileOk.Id)
	})
}

func TestDraDeleteFalse(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mockDraOp := NewMockDraOp(ctl)
	duplDra := NewDuplDra(mockDraOp)

	mockDraOp.EXPECT().LookupDuplById(dupl.Id).Return(dupl, nil)
	mockDraOp.EXPECT().RemoveDupl(dupl.Id).Return(nil)
	mockDraOp.EXPECT().DecRefCnt(dupl.Ref).Return(ref, nil)

	Convey("Delete file", t, func() {
		ok, eid, err := duplDra.Delete(dupl.Id)
		So(err, ShouldBeNil)
		So(ok, ShouldBeFalse)
		So(eid, ShouldEqual, "")
	})
}
