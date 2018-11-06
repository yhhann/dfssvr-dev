// mockgen -source=fileop.go > mock_fileop.go
// go test -v jingoal.com/dfs/fileop -run ^TestTee.*$
package fileop

import (
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/proto/transfer"
)

var (
	rId          = "564edc6965d7caed29056b5b"
	did          = "56a04788964e5650259db848"
	domain int64 = 2

	info = &transfer.FileInfo{
		Name:   "test-file",
		Domain: 2,
		User:   1001,
		Biz:    "mock",
	}

	rInfo = *info
)

func TestTeeCreate(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	major := NewMockDFSFileHandler(ctl)
	minor := NewMockDFSFileMinorHandler(ctl)
	handler := NewTeeHandler(major, minor)

	mockFile := NewMockDFSFile(ctl)
	rInfo.Id = rId
	mockFile.EXPECT().GetFileInfo().Return(&rInfo)

	major.EXPECT().Create(info).Return(mockFile, nil)
	minor.EXPECT().CreateWithGivenId(&rInfo).Return(mockFile, nil)

	Convey("Create a file.", t, func() {
		tf, err := handler.Create(info)
		So(err, ShouldBeNil)
		f, ok := tf.(*TeeFile)
		if ok {
			So(f.majorFile, ShouldNotBeNil)
			So(f.minorFile, ShouldNotBeNil)
		}

	})
}

func TestTeeOpen(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	major := NewMockDFSFileHandler(ctl)
	minor := NewMockDFSFileMinorHandler(ctl)
	handler := NewTeeHandler(major, minor)

	mockFile := NewMockDFSFile(ctl)

	major.EXPECT().Open(rId, domain).Return(mockFile, nil)
	minor.EXPECT().Open(rId, domain).Return(mockFile, nil)

	Convey("Open a file.", t, func() {
		tf, err := handler.Open(rId, domain)
		So(err, ShouldBeNil)
		f, ok := tf.(*TeeFile)
		if ok {
			So(f.majorFile, ShouldNotBeNil)
			So(f.minorFile, ShouldNotBeNil)
		}
	})
}

func TestTeeDuplicate(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	major := NewMockDFSFileHandler(ctl)
	minor := NewMockDFSFileMinorHandler(ctl)
	handler := NewTeeHandler(major, minor)

	major.EXPECT().Duplicate(rId).Return(did, nil)
	minor.EXPECT().DuplicateWithGivenId(rId, did).Return(did, nil)

	Convey("Duplicate a file.", t, func() {
		id, err := handler.Duplicate(rId)
		So(err, ShouldBeNil)
		So(id, ShouldEqual, did)
	})
}

func TestTeeRemove(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	major := NewMockDFSFileHandler(ctl)
	minor := NewMockDFSFileMinorHandler(ctl)
	handler := NewTeeHandler(major, minor)

	f := &meta.File{
		Id: rId,
	}

	major.EXPECT().Remove(rId, domain).Return(true, f, nil)
	minor.EXPECT().Remove(rId, domain).Return(true, f, nil)

	Convey("Remove file.", t, func() {
		r, m, err := handler.Remove(rId, domain)
		So(err, ShouldBeNil)
		So(r, ShouldBeTrue)
		So(m, ShouldNotBeNil)
	})

}
