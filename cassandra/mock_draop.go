// Code generated by MockGen. DO NOT EDIT.
// Source: draop.go

package cassandra

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockDraOp is a mock of DraOp interface
type MockDraOp struct {
	ctrl     *gomock.Controller
	recorder *MockDraOpMockRecorder
}

// MockDraOpMockRecorder is the mock recorder for MockDraOp
type MockDraOpMockRecorder struct {
	mock *MockDraOp
}

// NewMockDraOp creates a new mock instance
func NewMockDraOp(ctrl *gomock.Controller) *MockDraOp {
	mock := &MockDraOp{ctrl: ctrl}
	mock.recorder = &MockDraOpMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (_m *MockDraOp) EXPECT() *MockDraOpMockRecorder {
	return _m.recorder
}

// LookupFileById mocks base method
func (_m *MockDraOp) LookupFileById(id string) (*File, error) {
	ret := _m.ctrl.Call(_m, "LookupFileById", id)
	ret0, _ := ret[0].(*File)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LookupFileById indicates an expected call of LookupFileById
func (_mr *MockDraOpMockRecorder) LookupFileById(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "LookupFileById", reflect.TypeOf((*MockDraOp)(nil).LookupFileById), arg0)
}

// LookupFileByMd5 mocks base method
func (_m *MockDraOp) LookupFileByMd5(md5 string, domain int64) (*File, error) {
	ret := _m.ctrl.Call(_m, "LookupFileByMd5", md5, domain)
	ret0, _ := ret[0].(*File)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LookupFileByMd5 indicates an expected call of LookupFileByMd5
func (_mr *MockDraOpMockRecorder) LookupFileByMd5(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "LookupFileByMd5", reflect.TypeOf((*MockDraOp)(nil).LookupFileByMd5), arg0, arg1)
}

// SaveFile mocks base method
func (_m *MockDraOp) SaveFile(f *File) error {
	ret := _m.ctrl.Call(_m, "SaveFile", f)
	ret0, _ := ret[0].(error)
	return ret0
}

// SaveFile indicates an expected call of SaveFile
func (_mr *MockDraOpMockRecorder) SaveFile(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "SaveFile", reflect.TypeOf((*MockDraOp)(nil).SaveFile), arg0)
}

// RemoveFile mocks base method
func (_m *MockDraOp) RemoveFile(id string) error {
	ret := _m.ctrl.Call(_m, "RemoveFile", id)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoveFile indicates an expected call of RemoveFile
func (_mr *MockDraOpMockRecorder) RemoveFile(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "RemoveFile", reflect.TypeOf((*MockDraOp)(nil).RemoveFile), arg0)
}

// SaveDupl mocks base method
func (_m *MockDraOp) SaveDupl(dupl *Dupl) error {
	ret := _m.ctrl.Call(_m, "SaveDupl", dupl)
	ret0, _ := ret[0].(error)
	return ret0
}

// SaveDupl indicates an expected call of SaveDupl
func (_mr *MockDraOpMockRecorder) SaveDupl(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "SaveDupl", reflect.TypeOf((*MockDraOp)(nil).SaveDupl), arg0)
}

// LookupDuplById mocks base method
func (_m *MockDraOp) LookupDuplById(id string) (*Dupl, error) {
	ret := _m.ctrl.Call(_m, "LookupDuplById", id)
	ret0, _ := ret[0].(*Dupl)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LookupDuplById indicates an expected call of LookupDuplById
func (_mr *MockDraOpMockRecorder) LookupDuplById(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "LookupDuplById", reflect.TypeOf((*MockDraOp)(nil).LookupDuplById), arg0)
}

// LookupDuplByRefid mocks base method
func (_m *MockDraOp) LookupDuplByRefid(rid string) []*Dupl {
	ret := _m.ctrl.Call(_m, "LookupDuplByRefid", rid)
	ret0, _ := ret[0].([]*Dupl)
	return ret0
}

// LookupDuplByRefid indicates an expected call of LookupDuplByRefid
func (_mr *MockDraOpMockRecorder) LookupDuplByRefid(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "LookupDuplByRefid", reflect.TypeOf((*MockDraOp)(nil).LookupDuplByRefid), arg0)
}

// RemoveDupl mocks base method
func (_m *MockDraOp) RemoveDupl(id string) error {
	ret := _m.ctrl.Call(_m, "RemoveDupl", id)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoveDupl indicates an expected call of RemoveDupl
func (_mr *MockDraOpMockRecorder) RemoveDupl(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "RemoveDupl", reflect.TypeOf((*MockDraOp)(nil).RemoveDupl), arg0)
}

// SaveRef mocks base method
func (_m *MockDraOp) SaveRef(ref *Ref) error {
	ret := _m.ctrl.Call(_m, "SaveRef", ref)
	ret0, _ := ret[0].(error)
	return ret0
}

// SaveRef indicates an expected call of SaveRef
func (_mr *MockDraOpMockRecorder) SaveRef(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "SaveRef", reflect.TypeOf((*MockDraOp)(nil).SaveRef), arg0)
}

// LookupRefById mocks base method
func (_m *MockDraOp) LookupRefById(id string) (*Ref, error) {
	ret := _m.ctrl.Call(_m, "LookupRefById", id)
	ret0, _ := ret[0].(*Ref)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LookupRefById indicates an expected call of LookupRefById
func (_mr *MockDraOpMockRecorder) LookupRefById(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "LookupRefById", reflect.TypeOf((*MockDraOp)(nil).LookupRefById), arg0)
}

// RemoveRef mocks base method
func (_m *MockDraOp) RemoveRef(id string) error {
	ret := _m.ctrl.Call(_m, "RemoveRef", id)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoveRef indicates an expected call of RemoveRef
func (_mr *MockDraOpMockRecorder) RemoveRef(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "RemoveRef", reflect.TypeOf((*MockDraOp)(nil).RemoveRef), arg0)
}

// IncRefCnt mocks base method
func (_m *MockDraOp) IncRefCnt(id string) (*Ref, error) {
	ret := _m.ctrl.Call(_m, "IncRefCnt", id)
	ret0, _ := ret[0].(*Ref)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IncRefCnt indicates an expected call of IncRefCnt
func (_mr *MockDraOpMockRecorder) IncRefCnt(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "IncRefCnt", reflect.TypeOf((*MockDraOp)(nil).IncRefCnt), arg0)
}

// DecRefCnt mocks base method
func (_m *MockDraOp) DecRefCnt(id string) (*Ref, error) {
	ret := _m.ctrl.Call(_m, "DecRefCnt", id)
	ret0, _ := ret[0].(*Ref)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DecRefCnt indicates an expected call of DecRefCnt
func (_mr *MockDraOpMockRecorder) DecRefCnt(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "DecRefCnt", reflect.TypeOf((*MockDraOp)(nil).DecRefCnt), arg0)
}

// HealthCheck mocks base method
func (_m *MockDraOp) HealthCheck(node string) error {
	ret := _m.ctrl.Call(_m, "HealthCheck", node)
	ret0, _ := ret[0].(error)
	return ret0
}

// HealthCheck indicates an expected call of HealthCheck
func (_mr *MockDraOpMockRecorder) HealthCheck(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "HealthCheck", reflect.TypeOf((*MockDraOp)(nil).HealthCheck), arg0)
}
