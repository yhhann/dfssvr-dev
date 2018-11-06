package sql

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/util"
)

var (
	id            = "597edb8f4ec50300d28915f6"
	fmd5          = "a94bd72069b8ced73f94667de235d04f8"
	fdomain int64 = 666

	ExtAttr1, ExtAttr2 string
	mp                 = map[string]string{"color": "red", "expire": "10"}
	ExtAttrArray       = make([]string, 2)
	fileDo             = &FileMetadataDO{
		FId:        id,
		Biz:        "TEST-NORMAL",
		Name:       "我是测试.txt",
		Md5:        fmd5,
		UserId:     "jg",
		Domain:     fdomain,
		Size:       2048,
		ChunkSize:  1024,
		UploadTime: time.Now(),
		ExtAttr:    mp,
		EntityType: meta.EntityGridFS,
		RefCount:   1,
	}
	_did = "897edb8f4ec50300d28915f8"
)

func init() {
	mp1 := map[string]string{"color": "red"}
	j1, _ := json.Marshal(mp1)
	ExtAttr1 = string(j1)
	ExtAttrArray[0] = ExtAttr1

	mp2 := map[string]string{"expire": "10"}
	j2, _ := json.Marshal(mp2)
	ExtAttr2 = string(j2)
	ExtAttrArray[1] = ExtAttr2
}

func TestSave(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDao := NewMockFileMetadataDAO(mockCtrl)
	mockMetaImpl := NewMetaImpl(mockDao)
	first := mockDao.EXPECT().AddFileMeta(fileDo).Return(1, nil)
	second := mockDao.EXPECT().AddExtAttr(gomock.Eq(id), gomock.Eq(ExtAttr1)).Return(1, nil).After(first)
	mockDao.EXPECT().AddExtAttr(gomock.Eq(id), gomock.Eq(ExtAttr2)).Return(1, nil).After(second)
	fm := MetaFile(fileDo)

	Convey("Save file ok.", t, func() {
		err := mockMetaImpl.Save(fm)
		So(err, ShouldBeNil)
	})

	mockDao.EXPECT().AddFileMeta(gomock.Eq(fileDo)).Return(0, FileAlreadyExists)
	Convey("Save file error.", t, func() {
		err := mockMetaImpl.Save(fm)
		So(err, ShouldNotBeNil)
	})
}

func TestFind(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDao := NewMockFileMetadataDAO(mockCtrl)
	mockMetaImpl := NewMetaImpl(mockDao)
	mockDao.EXPECT().FindFileMetaWithExtAttr(gomock.Eq(id)).Return(fileDo, nil)

	Convey("Find file ok.", t, func() {
		fm, err := mockMetaImpl.Find(id)
		So(err, ShouldBeNil)
		So(fm, ShouldNotBeNil)
		So(fm.Id, ShouldEqual, id)
	})
}

func TestFindByMd5(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDao := NewMockFileMetadataDAO(mockCtrl)
	mockMetaImpl := NewMetaImpl(mockDao)
	mockDao.EXPECT().FindFileMetaByMD5WithExtAttr(gomock.Eq(fmd5), gomock.Eq(fdomain)).Return(fileDo, nil)

	Convey("Find file by md5 and domain ok.", t, func() {
		fm, err := mockMetaImpl.FindByMd5(fmd5, fdomain)
		So(err, ShouldBeNil)
		So(fm, ShouldNotBeNil)
		So(fm.Md5, ShouldEqual, fmd5)
	})
}

func TestDuplicateWithId(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDao := NewMockFileMetadataDAO(mockCtrl)
	mockMetaImpl := NewMetaImpl(mockDao)

	mockDao.EXPECT().FindFileMeta(gomock.Eq(id)).Return(fileDo, nil)
	createdTime := time.Now()
	mockDao.EXPECT().AddDuplicateInfo(gomock.Eq(id), gomock.Not(""), gomock.Eq(createdTime)).Return(1, nil)
	mockDao.EXPECT().UpdateFMRefCntIncreOne(gomock.Eq(id)).Return(1, nil)

	Convey("Duplicate file.", t, func() {
		did, err := mockMetaImpl.DuplicateWithId(id, "", createdTime)
		So(err, ShouldBeNil)
		So(did, ShouldNotBeEmpty)
	})
}

func TestDuplicateWithIdNotNull(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDao := NewMockFileMetadataDAO(mockCtrl)
	mockMetaImpl := NewMetaImpl(mockDao)

	mockDao.EXPECT().FindFileMeta(gomock.Eq(id)).Return(fileDo, nil)
	createdTime := time.Now()

	mockDao.EXPECT().AddDuplicateInfo(gomock.Eq(id), gomock.Eq(_did), gomock.Eq(createdTime)).Return(1, nil)
	mockDao.EXPECT().UpdateFMRefCntIncreOne(gomock.Eq(id)).Return(1, nil)

	Convey("Duplicate file.", t, func() {
		did, err := mockMetaImpl.DuplicateWithId(id, _did, createdTime)
		So(err, ShouldBeNil)
		So(util.GetDuplId(_did), ShouldEqual, did)
	})
}

func TestDuplicateWithDId(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDao := NewMockFileMetadataDAO(mockCtrl)
	mockMetaImpl := NewMetaImpl(mockDao)

	mockDao.EXPECT().FindDupInfoByDId(gomock.Eq(_did)).Return(id, time.Now(), nil)
	mockDao.EXPECT().FindFileMeta(gomock.Eq(id)).Return(fileDo, nil)
	createdTime := time.Now()
	d_did := "797edb8f4ec50300d28915f7"
	mockDao.EXPECT().AddDuplicateInfo(gomock.Eq(id), gomock.Eq(d_did), gomock.Eq(createdTime)).Return(1, nil)
	mockDao.EXPECT().UpdateFMRefCntIncreOne(gomock.Eq(id)).Return(1, nil)

	Convey("Duplicate file.", t, func() {
		did, err := mockMetaImpl.DuplicateWithId(util.GetDuplId(_did), d_did, createdTime)
		So(err, ShouldBeNil)
		So(util.GetDuplId(d_did), ShouldEqual, did)
	})
}

func TestDeleteDup(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDao := NewMockFileMetadataDAO(mockCtrl)
	mockMetaImpl := NewMetaImpl(mockDao)

	mockDao.EXPECT().FindDupInfoByDId(gomock.Eq(id)).Return(id, time.Now(), nil)
	fileDo.RefCount = 2
	mockDao.EXPECT().FindFileMeta(gomock.Eq(id)).Return(fileDo, nil)
	fmt.Println(fileDo)
	mockDao.EXPECT().UpdateFileMetaRefCnt(gomock.Eq(id), gomock.Eq(fileDo.RefCount-1)).Return(1, nil)
	mockDao.EXPECT().DeleteDupInfoByDId(gomock.Eq(id)).Return(1, nil)

	Convey("Delete duplicate file.", t, func() {
		tobeDeleted, entityIdToBeDeleted, err := mockMetaImpl.Delete(util.GetDuplId(id))
		So(err, ShouldBeNil)
		So(tobeDeleted, ShouldBeFalse)
		So(entityIdToBeDeleted, ShouldBeEmpty)
	})
}

func TestDelete(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockDao := NewMockFileMetadataDAO(mockCtrl)
	mockMetaImpl := NewMetaImpl(mockDao)
	fileDo.RefCount = 1
	first := mockDao.EXPECT().FindFileMeta(gomock.Eq(id)).Return(fileDo, nil)
	second := mockDao.EXPECT().DeleteExtAttr(gomock.Eq(id)).Return(2, nil).After(first)
	third := mockDao.EXPECT().DeleteDupInfoByFid(gomock.Eq(id)).After(second)
	mockDao.EXPECT().DeleteFileMeta(gomock.Eq(id)).After(third)

	Convey("Delete file.", t, func() {
		tobeDeleted, entityIdToBeDeleted, err := mockMetaImpl.Delete(id)
		So(err, ShouldBeNil)
		So(tobeDeleted, ShouldBeTrue)
		So(entityIdToBeDeleted, ShouldEqual, id)
	})
}
