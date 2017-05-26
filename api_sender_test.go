package stats

import (
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/turbinelabs/api/service/stats"
	"github.com/turbinelabs/nonstdlib/ptr"
	tbntime "github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/test/assert"
	"github.com/turbinelabs/test/matcher"
)

func TestNewAPIStats(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockSvc := stats.NewMockStatsService(ctrl)

	s := NewAPIStats(mockSvc, "sourcery")

	sImpl, ok := s.(*xStats)
	assert.NonNil(t, sImpl)
	assert.True(t, ok)

	senderImpl, ok := sImpl.sender.(*apiSender)
	assert.NonNil(t, senderImpl)
	assert.True(t, ok)

	assert.SameInstance(t, senderImpl.svc, mockSvc)
	assert.Equal(t, senderImpl.source, "sourcery")
}

func testStatsWithScope(t *testing.T, scope string, f func(Stats)) stats.Stat {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	payloadCaptor := matcher.CaptureType(reflect.TypeOf(&stats.Payload{}))

	mockSvc := stats.NewMockStatsService(ctrl)
	mockSvc.EXPECT().Forward(payloadCaptor).Return(nil, nil)

	s := NewAPIStats(mockSvc, "sourcery")
	if scope != "" {
		s = s.Scope(scope)
	}

	before := tbntime.ToUnixMicro(time.Now())
	f(s)
	after := tbntime.ToUnixMicro(time.Now())

	payload := payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, payload.Source, "sourcery")
	assert.Equal(t, len(payload.Stats), 1)
	assert.True(t, before <= payload.Stats[0].Timestamp)
	assert.True(t, after >= payload.Stats[0].Timestamp)
	assert.Equal(t, len(payload.Stats[0].Tags), 0)

	return payload.Stats[0]
}

func testStats(t *testing.T, f func(Stats)) stats.Stat {
	return testStatsWithScope(t, "", f)
}

func TestStatsCount(t *testing.T) {
	st := testStats(t, func(s Stats) {
		s.Count("metric", 1)
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Value, 1.0)

	st = testStatsWithScope(t, "a/b/c", func(s Stats) {
		s.Count("metric", 2)
	})

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, *st.Value, 2.0)
}

func TestStatsGauge(t *testing.T) {
	st := testStats(t, func(s Stats) {
		s.Gauge("metric", 123)
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Value, 123.0)

	st = testStatsWithScope(t, "a/b/c", func(s Stats) {
		s.Gauge("metric", 200)
	})

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, *st.Value, 200.0)
}

func TestStatsTimingDuration(t *testing.T) {
	st := testStats(t, func(s Stats) {
		s.Timing("metric", 1234*time.Millisecond)
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Value, 1.234)

	st = testStatsWithScope(t, "a/b/c", func(s Stats) {
		s.Timing("metric", 2*time.Second)
	})

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, *st.Value, 2.0)
}

func TestStatsTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	payloadCaptor := matcher.CaptureType(reflect.TypeOf(&stats.Payload{}))

	mockSvc := stats.NewMockStatsService(ctrl)
	mockSvc.EXPECT().Forward(payloadCaptor).Return(nil, nil)

	s := NewAPIStats(mockSvc, "sourcery")

	s.Count("metric", 1, NewKVTag("a", "1"), NewKVTag("b", "2"))

	payload := payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, payload.Source, "sourcery")
	assert.Equal(t, len(payload.Stats), 1)
	assert.Equal(t, ptr.Float64Value(payload.Stats[0].Value), 1.0)
	assert.Equal(t, ptr.BoolValue(payload.Stats[0].IsGauge), false)
	assert.MapEqual(t, payload.Stats[0].Tags, map[string]string{"a": "1", "b": "2"})

	mockSvc.EXPECT().Forward(payloadCaptor).Return(nil, nil)
	s.Gauge("metric", 2, NewKVTag("c", "1"), NewKVTag("d", "2"))

	payload = payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, len(payload.Stats), 1)
	assert.Equal(t, ptr.Float64Value(payload.Stats[0].Value), 2.0)
	assert.Equal(t, ptr.BoolValue(payload.Stats[0].IsGauge), true)
	assert.MapEqual(t, payload.Stats[0].Tags, map[string]string{"c": "1", "d": "2"})

	mockSvc.EXPECT().Forward(payloadCaptor).Return(nil, nil)
	s.Timing("metric", 2*time.Second, NewKVTag("e", "1"), NewKVTag("f", "2"))

	payload = payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, len(payload.Stats), 1)
	assert.Equal(t, ptr.Float64Value(payload.Stats[0].Value), 2.0)
	assert.Equal(t, ptr.BoolValue(payload.Stats[0].IsGauge), false)
	assert.MapEqual(t, payload.Stats[0].Tags, map[string]string{"e": "1", "f": "2"})
}

func TestStatsClose(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockSvc := stats.NewMockStatsService(ctrl)
	mockSvc.EXPECT().Close().Return(nil)

	s := NewAPIStats(mockSvc, "sourcery")
	s.Close()
}
