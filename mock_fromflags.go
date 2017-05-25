// Automatically generated by MockGen. DO NOT EDIT!
// Source: fromflags.go

package stats

import (
	gomock "github.com/golang/mock/gomock"
)

// Mock of FromFlags interface
type MockFromFlags struct {
	ctrl     *gomock.Controller
	recorder *_MockFromFlagsRecorder
}

// Recorder for MockFromFlags (not exported)
type _MockFromFlagsRecorder struct {
	mock *MockFromFlags
}

func NewMockFromFlags(ctrl *gomock.Controller) *MockFromFlags {
	mock := &MockFromFlags{ctrl: ctrl}
	mock.recorder = &_MockFromFlagsRecorder{mock}
	return mock
}

func (_m *MockFromFlags) EXPECT() *_MockFromFlagsRecorder {
	return _m.recorder
}

func (_m *MockFromFlags) Validate() error {
	ret := _m.ctrl.Call(_m, "Validate")
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockFromFlagsRecorder) Validate() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Validate")
}

func (_m *MockFromFlags) Make() (Stats, error) {
	ret := _m.ctrl.Call(_m, "Make")
	ret0, _ := ret[0].(Stats)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFromFlagsRecorder) Make() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Make")
}

// Mock of statsFromFlags interface
type MockstatsFromFlags struct {
	ctrl     *gomock.Controller
	recorder *_MockstatsFromFlagsRecorder
}

// Recorder for MockstatsFromFlags (not exported)
type _MockstatsFromFlagsRecorder struct {
	mock *MockstatsFromFlags
}

func NewMockstatsFromFlags(ctrl *gomock.Controller) *MockstatsFromFlags {
	mock := &MockstatsFromFlags{ctrl: ctrl}
	mock.recorder = &_MockstatsFromFlagsRecorder{mock}
	return mock
}

func (_m *MockstatsFromFlags) EXPECT() *_MockstatsFromFlagsRecorder {
	return _m.recorder
}

func (_m *MockstatsFromFlags) Validate() error {
	ret := _m.ctrl.Call(_m, "Validate")
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockstatsFromFlagsRecorder) Validate() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Validate")
}

func (_m *MockstatsFromFlags) Make() (Stats, error) {
	ret := _m.ctrl.Call(_m, "Make")
	ret0, _ := ret[0].(Stats)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockstatsFromFlagsRecorder) Make() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Make")
}
