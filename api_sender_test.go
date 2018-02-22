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
	s.AddTags(NewKVTag(SourceTag, "sourcery"), NewKVTag(ZoneTag, "zone"))

	apiStatsImpl, ok := s.(*apiStats)
	assert.NonNil(t, apiStatsImpl)
	assert.True(t, ok)

	assert.NonNil(t, apiStatsImpl.apiSender)
	assert.True(t, ok)

	assert.SameInstance(t, apiStatsImpl.apiSender.svc, mockSvc)
	assert.Equal(t, apiStatsImpl.apiSender.source, "sourcery")
	assert.Equal(t, apiStatsImpl.apiSender.zone, "zone")
}

func testAPISender(t *testing.T, f func(Stats)) stats.Payload {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	payloadCaptor := matcher.CaptureType(reflect.TypeOf(&stats.Payload{}))

	mockSvc := stats.NewMockStatsService(ctrl)
	mockSvc.EXPECT().ForwardV2(payloadCaptor).Return(nil, nil)

	s := NewAPIStats(mockSvc)
	s.AddTags(NewKVTag(SourceTag, "sourcery"), NewKVTag(ZoneTag, "zone"))

	before := tbntime.ToUnixMilli(time.Now())
	f(s)
	after := tbntime.ToUnixMilli(time.Now())

	payload := payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, payload.Source, "sourcery")
	assert.Equal(t, payload.Zone, "zone")
	assert.Equal(t, len(payload.Stats), 1)
	assert.LessThanEqual(t, before, payload.Stats[0].Timestamp)
	assert.GreaterThanEqual(t, after, payload.Stats[0].Timestamp)
	assert.Equal(t, len(payload.Stats[0].Tags), 0)

	return *payload
}

func testAPISenderWithTimestampTag(t *testing.T, f func(Stats)) stats.Payload {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	payloadCaptor := matcher.CaptureType(reflect.TypeOf(&stats.Payload{}))

	mockSvc := stats.NewMockStatsService(ctrl)
	mockSvc.EXPECT().ForwardV2(payloadCaptor).Return(nil, nil)

	s := NewAPIStats(mockSvc)
	s.AddTags(NewKVTag(SourceTag, "sourcery"), NewKVTag(ZoneTag, "zone"))

	f(s)

	payload := payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, payload.Source, "sourcery")
	assert.Equal(t, payload.Zone, "zone")
	assert.Equal(t, len(payload.Stats), 1)

	return *payload
}

func TestAPISenderCount(t *testing.T) {
	st := testAPISender(t, func(s Stats) {
		s.Count("metric", 1)
	}).Stats[0]

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Count, 1.0)

	st = testAPISender(t, func(s Stats) {
		s.Count("a/b/c/metric", 2)
	}).Stats[0]

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, *st.Count, 2.0)

	st = testAPISenderWithTimestampTag(t, func(s Stats) {
		s.Count("metric", 1, NewKVTag(TimestampTag, "1500000000"))
	}).Stats[0]

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Count, 1.0)
	assert.Equal(t, st.Timestamp, int64(1500000000))
	assert.Equal(t, len(st.Tags), 0)

	before := time.Now()
	st = testAPISenderWithTimestampTag(t, func(s Stats) {
		s.Count("metric", 1, NewKVTag(TimestampTag, "not-a-time"))
	}).Stats[0]
	after := time.Now()

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Count, 1.0)
	assert.GreaterThanEqual(t, st.Timestamp, before.Unix()*1000)
	assert.LessThanEqual(t, st.Timestamp, (after.Unix()+1)*1000)
	assert.MapEqual(t, st.Tags, map[string]string{TimestampTag: "not-a-time"})
}

func TestAPISenderGauge(t *testing.T) {
	st := testAPISender(t, func(s Stats) {
		s.Gauge("metric", 123)
	}).Stats[0]

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Gauge, 123.0)

	st = testAPISender(t, func(s Stats) {
		s.Gauge("a/b/c/metric", 200)
	}).Stats[0]

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, *st.Gauge, 200.0)

	st = testAPISenderWithTimestampTag(t, func(s Stats) {
		s.Gauge("metric", 123, NewKVTag(TimestampTag, "1500000000"))
	}).Stats[0]

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Gauge, 123.0)
	assert.Equal(t, st.Timestamp, int64(1500000000))
	assert.Equal(t, len(st.Tags), 0)
}

func TestAPISenderTimingDuration(t *testing.T) {
	st := testAPISender(t, func(s Stats) {
		s.Timing("metric", 1234*time.Millisecond)
	}).Stats[0]

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Gauge, 1.234)

	st = testAPISender(t, func(s Stats) {
		s.Timing("a/b/c/metric", 2*time.Second)
	}).Stats[0]

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, *st.Gauge, 2.0)

	st = testAPISenderWithTimestampTag(t, func(s Stats) {
		s.Timing("metric", 1234*time.Millisecond, NewKVTag(TimestampTag, "1500000000"))
	}).Stats[0]

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, *st.Gauge, 1.234)
	assert.Equal(t, st.Timestamp, int64(1500000000))
	assert.Equal(t, len(st.Tags), 0)
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

	payload := testAPISenderWithTimestampTag(t, func(s Stats) {
		sender := s.(*apiStats).apiSender
		sender.LatchedHistogram("histo", latchedHistogram, TimestampTag+"=1500000000")
	})
	st := payload.Stats[0]

	assert.Equal(t, st.Name, "histo")
	assert.MapEqual(
		t,
		payload.Limits,
		map[string][]float64{
			"default": {0.001, 0.002, 0.004, 0.008},
		},
	)
	assert.ArrayEqual(t, st.Histogram.Buckets, []int64{200, 550, 245, 4})
	assert.Equal(t, st.Histogram.Count, int64(1000))
	assert.Equal(t, st.Histogram.Sum, 1.778)
	assert.Equal(t, st.Histogram.Minimum, 0.0005)
	assert.Equal(t, st.Histogram.Maximum, 0.1)
	assert.Equal(t, st.Timestamp, int64(1500000000))
}

func TestAPISenderTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	payloadCaptor := matcher.CaptureType(reflect.TypeOf(&stats.Payload{}))

	mockSvc := stats.NewMockStatsService(ctrl)

	s := NewAPIStats(mockSvc)
	s.AddTags(
		NewKVTag(SourceTag, "sourcery"),
		NewKVTag(ZoneTag, "zone"),
		NewKVTag(ProxyTag, "default-proxy"),
	)

	mockSvc.EXPECT().ForwardV2(payloadCaptor).Return(nil, nil)
	s.Count(
		"metric",
		1,
		NewKVTag("a", "1"),
		NewKVTag("b", "2"),
		NewKVTag(ProxyTag, "proximate"),
		NewKVTag(ProxyVersionTag, "1.2.3"),
	)

	payload := payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, payload.Source, "sourcery")
	assert.Equal(t, payload.Zone, "zone")
	assert.Equal(t, ptr.StringValue(payload.Proxy), "proximate")
	assert.Equal(t, ptr.StringValue(payload.ProxyVersion), "1.2.3")
	assert.Equal(t, len(payload.Stats), 1)
	assert.Equal(t, ptr.Float64Value(payload.Stats[0].Count), 1.0)
	assert.MapEqual(t, payload.Stats[0].Tags, map[string]string{"a": "1", "b": "2"})

	mockSvc.EXPECT().ForwardV2(payloadCaptor).Return(nil, nil)
	s.Count(
		"metric",
		1,
		NewKVTag("a", "1"),
		NewKVTag("b", "2"),
	)

	payload = payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, payload.Source, "sourcery")
	assert.Equal(t, payload.Zone, "zone")
	assert.Equal(t, ptr.StringValue(payload.Proxy), "default-proxy")
	assert.Nil(t, payload.ProxyVersion)
	assert.Equal(t, len(payload.Stats), 1)
	assert.Equal(t, ptr.Float64Value(payload.Stats[0].Count), 1.0)
	assert.MapEqual(t, payload.Stats[0].Tags, map[string]string{"a": "1", "b": "2"})

	mockSvc.EXPECT().ForwardV2(payloadCaptor).Return(nil, nil)
	s.Gauge(
		"metric",
		2,
		NewKVTag("c", "1"),
		NewKVTag("d", "2"),
		NewKVTag(ProxyTag, "proximal"),
		NewKVTag(ProxyVersionTag, "2.3.4"),
	)

	payload = payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, ptr.StringValue(payload.Proxy), "proximal")
	assert.Equal(t, ptr.StringValue(payload.ProxyVersion), "2.3.4")
	assert.Equal(t, len(payload.Stats), 1)
	assert.Equal(t, ptr.Float64Value(payload.Stats[0].Gauge), 2.0)
	assert.MapEqual(t, payload.Stats[0].Tags, map[string]string{"c": "1", "d": "2"})

	mockSvc.EXPECT().ForwardV2(payloadCaptor).Return(nil, nil)
	s.Timing("metric", 2*time.Second, NewKVTag("e", "1"), NewKVTag("f", "2"))

	payload = payloadCaptor.V.(*stats.Payload)
	assert.Equal(t, len(payload.Stats), 1)
	assert.Equal(t, ptr.Float64Value(payload.Stats[0].Gauge), 2.0)
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
