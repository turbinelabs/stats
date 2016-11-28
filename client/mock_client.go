// Automatically generated by MockGen. DO NOT EDIT!
// Source: client.go

package client

import (
	gomock "github.com/golang/mock/gomock"
	executor "github.com/turbinelabs/nonstdlib/executor"
	stats0 "github.com/turbinelabs/nonstdlib/stats"
	stats "github.com/turbinelabs/stats"
)

// Mock of StatsClient interface
type MockStatsClient struct {
	ctrl     *gomock.Controller
	recorder *_MockStatsClientRecorder
}

// Recorder for MockStatsClient (not exported)
type _MockStatsClientRecorder struct {
	mock *MockStatsClient
}

func NewMockStatsClient(ctrl *gomock.Controller) *MockStatsClient {
	mock := &MockStatsClient{ctrl: ctrl}
	mock.recorder = &_MockStatsClientRecorder{mock}
	return mock
}

func (_m *MockStatsClient) EXPECT() *_MockStatsClientRecorder {
	return _m.recorder
}

func (_m *MockStatsClient) Forward(_param0 *stats.StatsPayload) (*stats.Result, error) {
	ret := _m.ctrl.Call(_m, "Forward", _param0)
	ret0, _ := ret[0].(*stats.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockStatsClientRecorder) Forward(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Forward", arg0)
}

func (_m *MockStatsClient) Close() error {
	ret := _m.ctrl.Call(_m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockStatsClientRecorder) Close() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Close")
}

func (_m *MockStatsClient) Stats(source string, scope ...string) stats0.Stats {
	_s := []interface{}{source}
	for _, _x := range scope {
		_s = append(_s, _x)
	}
	ret := _m.ctrl.Call(_m, "Stats", _s...)
	ret0, _ := ret[0].(stats0.Stats)
	return ret0
}

func (_mr *_MockStatsClientRecorder) Stats(arg0 interface{}, arg1 ...interface{}) *gomock.Call {
	_s := append([]interface{}{arg0}, arg1...)
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Stats", _s...)
}

// Mock of internalStatsClient interface
type MockinternalStatsClient struct {
	ctrl     *gomock.Controller
	recorder *_MockinternalStatsClientRecorder
}

// Recorder for MockinternalStatsClient (not exported)
type _MockinternalStatsClientRecorder struct {
	mock *MockinternalStatsClient
}

func NewMockinternalStatsClient(ctrl *gomock.Controller) *MockinternalStatsClient {
	mock := &MockinternalStatsClient{ctrl: ctrl}
	mock.recorder = &_MockinternalStatsClientRecorder{mock}
	return mock
}

func (_m *MockinternalStatsClient) EXPECT() *_MockinternalStatsClientRecorder {
	return _m.recorder
}

func (_m *MockinternalStatsClient) IssueRequest(_param0 *stats.StatsPayload, _param1 executor.CallbackFunc) error {
	ret := _m.ctrl.Call(_m, "IssueRequest", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockinternalStatsClientRecorder) IssueRequest(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "IssueRequest", arg0, arg1)
}
