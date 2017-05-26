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

func TestMultiGauge(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	multi, a, b := mkMulti(ctrl)
	a.EXPECT().Gauge("foo", 1.0, NewKVTag("a", "b"))
	b.EXPECT().Gauge("foo", 1.0, NewKVTag("a", "b"))
	multi.Gauge("foo", 1.0, NewKVTag("a", "b"))
}

func TestMultiCount(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	multi, a, b := mkMulti(ctrl)
	a.EXPECT().Count("foo", 1.0, NewKVTag("a", "b"))
	b.EXPECT().Count("foo", 1.0, NewKVTag("a", "b"))
	multi.Count("foo", 1.0, NewKVTag("a", "b"))
}

func TestMultiHistogram(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	multi, a, b := mkMulti(ctrl)
	a.EXPECT().Histogram("foo", 1.0, NewKVTag("a", "b"))
	b.EXPECT().Histogram("foo", 1.0, NewKVTag("a", "b"))
	multi.Histogram("foo", 1.0, NewKVTag("a", "b"))
}

func TestMultiTiming(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	multi, a, b := mkMulti(ctrl)
	a.EXPECT().Timing("foo", time.Second, NewKVTag("a", "b"))
	b.EXPECT().Timing("foo", time.Second, NewKVTag("a", "b"))
	multi.Timing("foo", time.Second, NewKVTag("a", "b"))
}

func TestMultiAddTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	multi, a, b := mkMulti(ctrl)
	a.EXPECT().AddTags(NewKVTag("a", "b"), NewKVTag("c", "d"))
	b.EXPECT().AddTags(NewKVTag("a", "b"), NewKVTag("c", "d"))
	multi.AddTags(NewKVTag("a", "b"), NewKVTag("c", "d"))
}

func TestMultiScope(t *testing.T) {
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

func TestMultiClose(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	a := NewMockStats(ctrl)
	b := NewMockStats(ctrl)
	c := NewMockStats(ctrl)

	multi := NewMulti(a, b, c)

	a.EXPECT().Close().Return(nil)
	b.EXPECT().Close().Return(errors.New("oh noes"))
	c.EXPECT().Close().Return(nil)

	assert.ErrorContains(t, multi.Close(), "oh noes")
}
