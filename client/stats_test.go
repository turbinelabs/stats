package client

import (
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	tbntime "github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/stats"
	"github.com/turbinelabs/test/assert"
	"github.com/turbinelabs/test/matcher"
)

func TestNewStats(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockClient := NewMockStatsClient(ctrl)

	s := newStats(mockClient, "sourcery")
	sImpl, ok := s.(*statsT)
	assert.NonNil(t, sImpl)
	assert.True(t, ok)
	assert.SameInstance(t, sImpl.client, mockClient)
	assert.Equal(t, sImpl.source, "sourcery")
	assert.Equal(t, sImpl.scope, "")
}

func testStatsWithScope(t *testing.T, scope string, f func(*statsT) error) stats.Stat {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	payloadCaptor := matcher.CaptureType(reflect.TypeOf(&stats.StatsPayload{}))

	mockClient := NewMockStatsClient(ctrl)
	mockClient.EXPECT().Forward(payloadCaptor).Return(nil, nil)

	s := &statsT{client: mockClient, source: "sourcery", scope: scope}

	before := tbntime.ToUnixMicro(time.Now())
	err := f(s)
	after := tbntime.ToUnixMicro(time.Now())

	assert.Nil(t, err)

	payload := payloadCaptor.V.(*stats.StatsPayload)
	assert.Equal(t, payload.Source, "sourcery")
	assert.Equal(t, len(payload.Stats), 1)
	assert.True(t, before <= payload.Stats[0].Timestamp)
	assert.True(t, after >= payload.Stats[0].Timestamp)
	assert.Equal(t, len(payload.Stats[0].Tags), 0)

	return payload.Stats[0]
}

func testStats(t *testing.T, f func(*statsT) error) stats.Stat {
	return testStatsWithScope(t, "", f)
}

func TestStatsInc(t *testing.T) {
	st := testStats(t, func(s *statsT) error {
		return s.Inc("metric", 1)
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, st.Value, 1.0)

	st = testStatsWithScope(t, "a/b/c", func(s *statsT) error {
		return s.Inc("metric", 2)
	})

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, st.Value, 2.0)
}

func TestStatsGauge(t *testing.T) {
	st := testStats(t, func(s *statsT) error {
		return s.Gauge("metric", 123)
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, st.Value, 123.0)

	st = testStatsWithScope(t, "a/b/c", func(s *statsT) error {
		return s.Gauge("metric", 200)
	})

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, st.Value, 200.0)
}

func TestStatsTimingDuration(t *testing.T) {
	st := testStats(t, func(s *statsT) error {
		return s.TimingDuration("metric", 1234*time.Millisecond)
	})

	assert.Equal(t, st.Name, "metric")
	assert.Equal(t, st.Value, 1.234)

	st = testStatsWithScope(t, "a/b/c", func(s *statsT) error {
		return s.TimingDuration("metric", 2*time.Second)
	})

	assert.Equal(t, st.Name, "a/b/c/metric")
	assert.Equal(t, st.Value, 2.0)
}
