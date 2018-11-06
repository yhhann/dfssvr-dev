// Code generated by MockGen. DO NOT EDIT.
// Source: sqlop.go

package sql

import (
	"reflect"
	"time"

	gomock "github.com/golang/mock/gomock"
)

// MockFileMetadataDAO is a mock of FileMetadataDAO interface
type MockFileMetadataDAO struct {
	ctrl     *gomock.Controller
	recorder *MockFileMetadataDAOMockRecorder
}

// MockFileMetadataDAOMockRecorder is the mock recorder for MockFileMetadataDAO
type MockFileMetadataDAOMockRecorder struct {
	mock *MockFileMetadataDAO
}

// NewMockFileMetadataDAO creates a new mock instance
func NewMockFileMetadataDAO(ctrl *gomock.Controller) *MockFileMetadataDAO {
	mock := &MockFileMetadataDAO{ctrl: ctrl}
	mock.recorder = &MockFileMetadataDAOMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (_m *MockFileMetadataDAO) EXPECT() *MockFileMetadataDAOMockRecorder {
	return _m.recorder
}

// AddFileMeta mocks base method
func (_m *MockFileMetadataDAO) AddFileMeta(fm *FileMetadataDO) (int, error) {
	ret := _m.ctrl.Call(_m, "AddFileMeta", fm)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddFileMeta indicates an expected call of AddFileMeta
func (_mr *MockFileMetadataDAOMockRecorder) AddFileMeta(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "AddFileMeta", reflect.TypeOf((*MockFileMetadataDAO)(nil).AddFileMeta), arg0)
}

// DeleteFileMeta mocks base method
func (_m *MockFileMetadataDAO) DeleteFileMeta(fId string) (int, error) {
	ret := _m.ctrl.Call(_m, "DeleteFileMeta", fId)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteFileMeta indicates an expected call of DeleteFileMeta
func (_mr *MockFileMetadataDAOMockRecorder) DeleteFileMeta(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "DeleteFileMeta", reflect.TypeOf((*MockFileMetadataDAO)(nil).DeleteFileMeta), arg0)
}

// UpdateFileMetaRefCnt mocks base method
func (_m *MockFileMetadataDAO) UpdateFileMetaRefCnt(fId string, refCount int) (int, error) {
	ret := _m.ctrl.Call(_m, "UpdateFileMetaRefCnt", fId, refCount)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateFileMetaRefCnt indicates an expected call of UpdateFileMetaRefCnt
func (_mr *MockFileMetadataDAOMockRecorder) UpdateFileMetaRefCnt(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "UpdateFileMetaRefCnt", reflect.TypeOf((*MockFileMetadataDAO)(nil).UpdateFileMetaRefCnt), arg0, arg1)
}

// FindFileMeta mocks base method
func (_m *MockFileMetadataDAO) FindFileMeta(fId string) (*FileMetadataDO, error) {
	ret := _m.ctrl.Call(_m, "FindFileMeta", fId)
	ret0, _ := ret[0].(*FileMetadataDO)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindFileMeta indicates an expected call of FindFileMeta
func (_mr *MockFileMetadataDAOMockRecorder) FindFileMeta(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "FindFileMeta", reflect.TypeOf((*MockFileMetadataDAO)(nil).FindFileMeta), arg0)
}

// FindFileMetaWithExtAttr mocks base method
func (_m *MockFileMetadataDAO) FindFileMetaWithExtAttr(fId string) (*FileMetadataDO, error) {
	ret := _m.ctrl.Call(_m, "FindFileMetaWithExtAttr", fId)
	ret0, _ := ret[0].(*FileMetadataDO)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindFileMetaWithExtAttr indicates an expected call of FindFileMetaWithExtAttr
func (_mr *MockFileMetadataDAOMockRecorder) FindFileMetaWithExtAttr(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "FindFileMetaWithExtAttr", reflect.TypeOf((*MockFileMetadataDAO)(nil).FindFileMetaWithExtAttr), arg0)
}

// FindFileMetaByMD5WithExtAttr mocks base method
func (_m *MockFileMetadataDAO) FindFileMetaByMD5WithExtAttr(md5 string, domain int64) (*FileMetadataDO, error) {
	ret := _m.ctrl.Call(_m, "FindFileMetaByMD5WithExtAttr", md5, domain)
	ret0, _ := ret[0].(*FileMetadataDO)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindFileMetaByMD5WithExtAttr indicates an expected call of FindFileMetaByMD5WithExtAttr
func (_mr *MockFileMetadataDAOMockRecorder) FindFileMetaByMD5WithExtAttr(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "FindFileMetaByMD5WithExtAttr", reflect.TypeOf((*MockFileMetadataDAO)(nil).FindFileMetaByMD5WithExtAttr), arg0, arg1)
}

// AddExtAttr mocks base method
func (_m *MockFileMetadataDAO) AddExtAttr(fId string, extAttr string) (int, error) {
	ret := _m.ctrl.Call(_m, "AddExtAttr", fId, extAttr)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddExtAttr indicates an expected call of AddExtAttr
func (_mr *MockFileMetadataDAOMockRecorder) AddExtAttr(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "AddExtAttr", reflect.TypeOf((*MockFileMetadataDAO)(nil).AddExtAttr), arg0, arg1)
}

// DeleteExtAttr mocks base method
func (_m *MockFileMetadataDAO) DeleteExtAttr(fId string) (int, error) {
	ret := _m.ctrl.Call(_m, "DeleteExtAttr", fId)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteExtAttr indicates an expected call of DeleteExtAttr
func (_mr *MockFileMetadataDAOMockRecorder) DeleteExtAttr(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "DeleteExtAttr", reflect.TypeOf((*MockFileMetadataDAO)(nil).DeleteExtAttr), arg0)
}

// FindExtAttrByFId mocks base method
func (_m *MockFileMetadataDAO) FindExtAttrByFId(fId string) ([]string, error) {
	ret := _m.ctrl.Call(_m, "FindExtAttrByFId", fId)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindExtAttrByFId indicates an expected call of FindExtAttrByFId
func (_mr *MockFileMetadataDAOMockRecorder) FindExtAttrByFId(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "FindExtAttrByFId", reflect.TypeOf((*MockFileMetadataDAO)(nil).FindExtAttrByFId), arg0)
}

// AddDuplicateInfo mocks base method
func (_m *MockFileMetadataDAO) AddDuplicateInfo(fId string, dId string, createDate time.Time) (int, error) {
	ret := _m.ctrl.Call(_m, "AddDuplicateInfo", fId, dId, createDate)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddDuplicateInfo indicates an expected call of AddDuplicateInfo
func (_mr *MockFileMetadataDAOMockRecorder) AddDuplicateInfo(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "AddDuplicateInfo", reflect.TypeOf((*MockFileMetadataDAO)(nil).AddDuplicateInfo), arg0, arg1, arg2)
}

// DeleteDupInfoByDId mocks base method
func (_m *MockFileMetadataDAO) DeleteDupInfoByDId(dId string) (int, error) {
	ret := _m.ctrl.Call(_m, "DeleteDupInfoByDId", dId)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteDupInfoByDId indicates an expected call of DeleteDupInfoByDId
func (_mr *MockFileMetadataDAOMockRecorder) DeleteDupInfoByDId(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "DeleteDupInfoByDId", reflect.TypeOf((*MockFileMetadataDAO)(nil).DeleteDupInfoByDId), arg0)
}

// DeleteDupInfoByFid mocks base method
func (_m *MockFileMetadataDAO) DeleteDupInfoByFid(fId string) (int, error) {
	ret := _m.ctrl.Call(_m, "DeleteDupInfoByFid", fId)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteDupInfoByFid indicates an expected call of DeleteDupInfoByFid
func (_mr *MockFileMetadataDAOMockRecorder) DeleteDupInfoByFid(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "DeleteDupInfoByFid", reflect.TypeOf((*MockFileMetadataDAO)(nil).DeleteDupInfoByFid), arg0)
}

// FindDupInfoByDId mocks base method
func (_m *MockFileMetadataDAO) FindDupInfoByDId(dId string) (string, time.Time, error) {
	ret := _m.ctrl.Call(_m, "FindDupInfoByDId", dId)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(time.Time)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// FindDupInfoByDId indicates an expected call of FindDupInfoByDId
func (_mr *MockFileMetadataDAOMockRecorder) FindDupInfoByDId(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "FindDupInfoByDId", reflect.TypeOf((*MockFileMetadataDAO)(nil).FindDupInfoByDId), arg0)
}

// UpdateFMRefCntIncreOne mocks base method
func (_m *MockFileMetadataDAO) UpdateFMRefCntIncreOne(fId string) (int, error) {
	ret := _m.ctrl.Call(_m, "UpdateFMRefCntIncreOne", fId)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateFMRefCntIncreOne indicates an expected call of UpdateFMRefCntIncreOne
func (_mr *MockFileMetadataDAOMockRecorder) UpdateFMRefCntIncreOne(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "UpdateFMRefCntIncreOne", reflect.TypeOf((*MockFileMetadataDAO)(nil).UpdateFMRefCntIncreOne), arg0)
}
