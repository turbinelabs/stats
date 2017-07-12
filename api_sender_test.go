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

	s := NewAPIStats(mockSvc)
	s.AddTags(NewKVTag(SourceTag, "sourcery"))

	apiStatsImpl, ok := s.(*apiStats)
	assert.NonNil(t, apiStatsImpl)
	assert.True(t, ok)

	assert.NonNil(t, apiStatsImpl.apiSender)
	assert.True(t, ok)

	assert.SameInstance(t, apiStatsImpl.apiSender.svc, mockSvc)
	assert.Equal(t, apiStatsImpl.apiSender.source, "sourcery")
}

func testAPISenderWithScope(t *testing.T, scope string, f func(Stats)) stats.Stat {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	payloadCaptor := matcher.CaptureType(reflect.TypeOf(&stats.Payload{}))

	mockSvc := stats.NewMockStatsService(ctrl)
	mockSvc.EXPECT().Forward(payloadCaptor).Return(nil, nil)

	s := NewAPIStats(mockSvc)
	s.AddTags(NewKVTag(SourceTag, "sourcery"))
	if scope != "" {
		s = s.Scope(scope)
	}

	before := tbntime.ToUnixMicro(time.Now())
	f(s)
	after := tbntime.ToUnixMicro(time.Now())

	payload := payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, payload.Source, "sourcery")
	assert.Equal(t, len(payload.Stats), 1)
	assert.LessThanEqual(t, before, payload.Stats[0].Timestamp)
	assert.GreaterThanEqual(t, after, payload.Stats[0].Timestamp)
	assert.Equal(t, len(payload.Stats[0].Tags), 0)

	return payload.Stats[0]
}

func testAPISenderWithTimestampTag(t *testing.T, f func(Stats)) stats.Stat {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	payloadCaptor := matcher.CaptureType(reflect.TypeOf(&stats.Payload{}))

	mockSvc := stats.NewMockStatsService(ctrl)
	mockSvc.EXPECT().Forward(payloadCaptor).Return(nil, nil)

	s := NewAPIStats(mockSvc)
	s.AddTags(NewKVTag(SourceTag, "sourcery"))

	f(s)

	payload := payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, payload.Source, "sourcery")
	assert.Equal(t, len(payload.Stats), 1)
	assert.Equal(t, len(payload.Stats[0].Tags), 0)

	return payload.Stats[0]
}

func testAPISender(t *testing.T, f func(Stats)) stats.Stat {
	return testAPISenderWithScope(t, "", f)
}

func TestAPISenderCount(t *testing.T) {
	st := testAPISender(t, func(s Stats) {
		s.Count("metric", 1)
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Value, 1.0)

	st = testAPISenderWithScope(t, "a/b/c", func(s Stats) {
		s.Count("metric", 2)
	})

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, *st.Value, 2.0)

	st = testAPISenderWithTimestampTag(t, func(s Stats) {
		s.Count("metric", 1, NewKVTag(TimestampTag, "1500000000000"))
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Value, 1.0)
	assert.Equal(t, st.Timestamp, int64(1500000000000000))
}

func TestAPISenderGauge(t *testing.T) {
	st := testAPISender(t, func(s Stats) {
		s.Gauge("metric", 123)
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Value, 123.0)

	st = testAPISenderWithScope(t, "a/b/c", func(s Stats) {
		s.Gauge("metric", 200)
	})

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, *st.Value, 200.0)

	st = testAPISenderWithTimestampTag(t, func(s Stats) {
		s.Gauge("metric", 123, NewKVTag(TimestampTag, "1500000000000"))
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Value, 123.0)
	assert.Equal(t, st.Timestamp, int64(1500000000000000))
}

func TestAPISenderTimingDuration(t *testing.T) {
	st := testAPISender(t, func(s Stats) {
		s.Timing("metric", 1234*time.Millisecond)
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Value, 1.234)

	st = testAPISenderWithScope(t, "a/b/c", func(s Stats) {
		s.Timing("metric", 2*time.Second)
	})

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, *st.Value, 2.0)

	st = testAPISenderWithTimestampTag(t, func(s Stats) {
		s.Timing("metric", 1234*time.Millisecond, NewKVTag(TimestampTag, "1500000000000"))
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Value, 1.234)
	assert.Equal(t, st.Timestamp, int64(1500000000000000))
}

func TestAPISenderLatchedHistogram(t *testing.T) {
	latchedHistogram := LatchedHistogram{
		BaseValue: 0.001,
		Buckets:   []int64{200, 550, 245, 4},
		Count:     1000,
		Sum:       1.778,
		Min:       0.0005,
		Max:       0.1,
	}

	st := testAPISenderWithTimestampTag(t, func(s Stats) {
		sender := s.(*apiStats).apiSender
		sender.LatchedHistogram("histo", latchedHistogram, TimestampTag+"=1500000000000")
	})

	assert.Equal(t, st.Name, "histo")
	assert.ArrayEqual(
		t,
		st.Histogram.Buckets,
		[][2]float64{
			{0.001, 200.0},
			{0.002, 550.0},
			{0.004, 245.0},
			{0.008, 4.0},
		},
	)
	assert.Equal(t, st.Histogram.Count, int64(1000))
	assert.Equal(t, st.Histogram.Sum, 1.778)
	assert.Equal(t, st.Histogram.Minimum, 0.0005)
	assert.Equal(t, st.Histogram.Maximum, 0.1)
	assert.Equal(t, st.Histogram.P50, 0.002)
	assert.Equal(t, st.Histogram.P99, 0.004)
	assert.Equal(t, st.Timestamp, int64(1500000000000000))
}

func TestAPISenderTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	payloadCaptor := matcher.CaptureType(reflect.TypeOf(&stats.Payload{}))

	mockSvc := stats.NewMockStatsService(ctrl)
	mockSvc.EXPECT().Forward(payloadCaptor).Return(nil, nil)

	s := NewAPIStats(mockSvc)
	s.AddTags(NewKVTag(SourceTag, "sourcery"))

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

func TestApiSenderClose(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockSvc := stats.NewMockStatsService(ctrl)
	mockSvc.EXPECT().Close().Return(nil)

	s := NewAPIStats(mockSvc)
	s.Close()
}

func TestAPICleanerToTagString(t *testing.T) {
	testCases := []struct {
		tag      Tag
		expected string
	}{
		{
			tag:      NewKVTag("x", "y"),
			expected: `x=y`,
		},
		{
			tag:      NewKVTag("=x=x=", "y"),
			expected: `xx=y`,
		},
	}

	for _, tc := range testCases {
		got := apiCleaner.tagToString(tc.tag)
		assert.Equal(t, got, tc.expected)
	}
}
