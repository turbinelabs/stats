package stats

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/turbinelabs/test/assert"
)

func mkMulti(ctrl *gomock.Controller) (Stats, *MockStats, *MockStats) {
	a := NewMockStats(ctrl)
	b := NewMockStats(ctrl)
	return NewMulti(a, b), a, b
}

func mkRollUp(ctrl *gomock.Controller) (Stats, *MockStats, *MockStats) {
	a := NewMockStats(ctrl)
	b := NewMockStats(ctrl)
	return &rollUpStats{self: b, parent: a}, a, b
}

func mkRootRollUp(ctrl *gomock.Controller) (Stats, *MockStats, *MockStats) {
	a := NewMockStats(ctrl)
	return &rollUpStats{self: a}, a, nil
}

func testMultiGauge(
	t *testing.T,
	mk func(ctrl *gomock.Controller) (Stats, *MockStats, *MockStats),
) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	s, a, b := mk(ctrl)
	if a != nil {
		a.EXPECT().Gauge("foo", 1.0, NewKVTag("a", "b"))
	}
	if b != nil {
		b.EXPECT().Gauge("foo", 1.0, NewKVTag("a", "b"))
	}
	s.Gauge("foo", 1.0, NewKVTag("a", "b"))
}

func testMultiCount(
	t *testing.T,
	mk func(ctrl *gomock.Controller) (Stats, *MockStats, *MockStats),
) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	s, a, b := mk(ctrl)
	if a != nil {
		a.EXPECT().Count("foo", 1.0, NewKVTag("a", "b"))
	}
	if b != nil {
		b.EXPECT().Count("foo", 1.0, NewKVTag("a", "b"))
	}
	s.Count("foo", 1.0, NewKVTag("a", "b"))
}

func testMultiHistogram(
	t *testing.T,
	mk func(ctrl *gomock.Controller) (Stats, *MockStats, *MockStats),
) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	s, a, b := mk(ctrl)
	if a != nil {
		a.EXPECT().Histogram("foo", 1.0, NewKVTag("a", "b"))
	}
	if b != nil {
		b.EXPECT().Histogram("foo", 1.0, NewKVTag("a", "b"))
	}
	s.Histogram("foo", 1.0, NewKVTag("a", "b"))
}

func testMultiTiming(
	t *testing.T,
	mk func(ctrl *gomock.Controller) (Stats, *MockStats, *MockStats),
) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	s, a, b := mk(ctrl)
	if a != nil {
		a.EXPECT().Timing("foo", time.Second, NewKVTag("a", "b"))
	}
	if b != nil {
		b.EXPECT().Timing("foo", time.Second, NewKVTag("a", "b"))
	}
	s.Timing("foo", time.Second, NewKVTag("a", "b"))
}

func TestMultiStats(t *testing.T) {
	testMultiGauge(t, mkMulti)
	testMultiCount(t, mkMulti)
	testMultiHistogram(t, mkMulti)
	testMultiTiming(t, mkMulti)
}

func TestMultiStatsAddTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	s, a, b := mkMulti(ctrl)
	a.EXPECT().AddTags(NewKVTag("a", "b"), NewKVTag("c", "d"))
	b.EXPECT().AddTags(NewKVTag("a", "b"), NewKVTag("c", "d"))
	s.AddTags(NewKVTag("a", "b"), NewKVTag("c", "d"))
}

func TestMultiStatsClose(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	s, a, b := mkMulti(ctrl)
	a.EXPECT().Close().Return(errors.New("oh noes"))
	b.EXPECT().Close().Return(errors.New("second error"))

	assert.ErrorContains(t, s.Close(), "oh noes")

	s, a, b = mkMulti(ctrl)
	a.EXPECT().Close().Return(nil)
	b.EXPECT().Close().Return(errors.New("oh noes"))

	assert.ErrorContains(t, s.Close(), "oh noes")
}

func TestMultiStatsScope(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	multi, a, b := mkMulti(ctrl)
	_, scopedA, scopedB := mkMulti(ctrl)

	a.EXPECT().Scope("a", "b", "c").Return(scopedA)
	b.EXPECT().Scope("a", "b", "c").Return(scopedB)

	scopedMulti := multi.Scope("a", "b", "c")

	scopedA.EXPECT().Count("x", 1.0)
	scopedB.EXPECT().Count("x", 1.0)

	scopedMulti.Count("x", 1.0)
}

func TestRollUpStats(t *testing.T) {
	testMultiGauge(t, mkRollUp)
	testMultiCount(t, mkRollUp)
	testMultiHistogram(t, mkRollUp)
	testMultiTiming(t, mkRollUp)
}

func TestRollUpStatsRoot(t *testing.T) {
	testMultiGauge(t, mkRootRollUp)
	testMultiCount(t, mkRootRollUp)
	testMultiHistogram(t, mkRootRollUp)
	testMultiTiming(t, mkRootRollUp)
}

func TestRollUpStatsScope(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	rootMock := NewMockStats(ctrl)
	aMock := NewMockStats(ctrl)
	bMock := NewMockStats(ctrl)
	cMock := NewMockStats(ctrl)

	rootMock.EXPECT().Scope("a").Return(aMock)
	aMock.EXPECT().Scope("b").Return(bMock)
	bMock.EXPECT().Scope("c").Return(cMock)

	root := NewRollUp(rootMock)
	scoped := root.Scope("a", "b", "c")

	rootMock.EXPECT().Count("x", 1.0, NewKVTag("tag", "value"))
	aMock.EXPECT().Count("x", 1.0, NewKVTag("tag", "value"))
	bMock.EXPECT().Count("x", 1.0, NewKVTag("tag", "value"))
	cMock.EXPECT().Count("x", 1.0, NewKVTag("tag", "value"))

	scoped.Count("x", 1.0, NewKVTag("tag", "value"))
}

func TestRollUpStatsScopeOneByOne(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	rootMock := NewMockStats(ctrl)
	aMock := NewMockStats(ctrl)
	bMock := NewMockStats(ctrl)
	cMock := NewMockStats(ctrl)

	rootMock.EXPECT().Scope("a").Return(aMock)
	aMock.EXPECT().Scope("b").Return(bMock)
	bMock.EXPECT().Scope("c").Return(cMock)

	root := NewRollUp(rootMock)
	scoped := root.Scope("a").Scope("b").Scope("c")

	rootMock.EXPECT().Count("x", 1.0, NewKVTag("tag", "value"))
	aMock.EXPECT().Count("x", 1.0, NewKVTag("tag", "value"))
	bMock.EXPECT().Count("x", 1.0, NewKVTag("tag", "value"))
	cMock.EXPECT().Count("x", 1.0, NewKVTag("tag", "value"))

	scoped.Count("x", 1.0, NewKVTag("tag", "value"))
}

func TestRollUpStatsCachesScopes(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	rootMock := NewMockStats(ctrl)
	aMock := NewMockStats(ctrl)
	bMock := NewMockStats(ctrl)

	rootMock.EXPECT().Scope("a").Times(1).Return(aMock)
	aMock.EXPECT().Scope("b").Times(1).Return(bMock)

	root := NewRollUp(rootMock)
	scoped := root.Scope("a", "b")
	scopedAgain := root.Scope("a", "b")

	assert.SameInstance(t, scopedAgain, scoped)
}

func TestRollUpStatsAddTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	s, _, child := mkRollUp(ctrl)
	child.EXPECT().AddTags(NewKVTag("a", "b"), NewKVTag("c", "d"))
	s.AddTags(NewKVTag("a", "b"), NewKVTag("c", "d"))
}

func TestRollUpStatsClose(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	s, _, child := mkRollUp(ctrl)
	child.EXPECT().Close().Return(errors.New("oh noes"))

	assert.ErrorContains(t, s.Close(), "oh noes")
}
