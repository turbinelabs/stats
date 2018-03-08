package stats

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/turbinelabs/nonstdlib/ptr"
	tbntime "github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/test/assert"
)

type testHistogram struct {
	buckets []int64
	count   int64
	sum     float64
	min     float64
	max     float64
}

var (
	testCleaner = cleaner{
		cleanStatName: identity,
		cleanTagName:  identity,
		cleanTagValue: identity,
		tagDelim:      "=",
		scopeDelim:    ".",
	}

	measurements = []float64{
		0.001, 0.0085, 0.0115, 0.020,
		0.002, 0.0100, 0.0100, 0.021,
		0.003, 0.0100, 0.0100, 0.022,
		0.004, 0.0100, 0.0100, 0.023,
		0.005, 0.0100, 0.0100, 0.024,
	}

	histograms = []*testHistogram{
		{
			buckets: []int64{1, 0, 0, 0, 2, 1, 0, 0, 0, 0},
			count:   4,
			sum:     0.001 + 0.0085 + 0.0115 + 0.020,
			min:     0.001,
			max:     0.020,
		},
		{
			buckets: []int64{0, 1, 0, 0, 2, 1, 0, 0, 0, 0},
			count:   4,
			sum:     0.002 + 0.0100 + 0.0100 + 0.021,
			min:     0.002,
			max:     0.021,
		},
		{
			buckets: []int64{0, 0, 1, 0, 2, 1, 0, 0, 0, 0},
			count:   4,
			sum:     0.003 + 0.0100 + 0.0100 + 0.022,
			min:     0.003,
			max:     0.022,
		},
		{
			buckets: []int64{0, 0, 1, 0, 2, 1, 0, 0, 0, 0},
			count:   4,
			sum:     0.004 + 0.0100 + 0.0100 + 0.023,
			min:     0.004,
			max:     0.023,
		},
		{
			buckets: []int64{0, 0, 0, 1, 2, 1, 0, 0, 0, 0},
			count:   4,
			sum:     0.005 + 0.0100 + 0.0100 + 0.024,
			min:     0.005,
			max:     0.024,
		},
	}
)

func TestNewLatchingSender(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	underlying := newMockXstatsSender(ctrl)

	s := newLatchingSender(underlying, testCleaner)
	sImpl, ok := s.(*latchingSender)
	assert.True(t, ok)
	assert.SameInstance(t, sImpl.underlying, underlying)
	assert.NonNil(t, sImpl.lock)
	assert.Equal(t, sImpl.latchWindow, DefaultLatchWindow)
	assert.Equal(t, sImpl.numHistogramBuckets, DefaultHistogramNumBuckets)
	assert.Equal(t, sImpl.baseHistogramValue, DefaultHistogramBaseValue)
	assert.NonNil(t, sImpl.timeSource)
	assert.True(t, sImpl.latchStart.IsZero())
	assert.Nil(t, sImpl.counters)
	assert.Nil(t, sImpl.gauges)
	assert.Nil(t, sImpl.histograms)

	s = newLatchingSender(underlying, testCleaner, latchWindow(10*time.Second))
	sImpl = s.(*latchingSender)
	assert.Equal(t, sImpl.latchWindow, 10*time.Second)
	assert.Equal(t, sImpl.numHistogramBuckets, DefaultHistogramNumBuckets)
	assert.Equal(t, sImpl.baseHistogramValue, DefaultHistogramBaseValue)

	s = newLatchingSender(underlying, testCleaner, latchBuckets(1000000000.0, 5))
	sImpl = s.(*latchingSender)
	assert.Equal(t, sImpl.latchWindow, DefaultLatchWindow)
	assert.Equal(t, sImpl.numHistogramBuckets, 5)
	assert.Equal(t, sImpl.baseHistogramValue, 1000000000.0)

	tbntime.WithCurrentTimeFrozen(func(tc tbntime.ControlledSource) {
		s := newLatchingSender(underlying, testCleaner, timeSource(tc))
		sImpl := s.(*latchingSender)
		assert.SameInstance(t, sImpl.timeSource, tc)
	})
}

// Params:
//   t -- testing.T
//   inputMetric: the name of the stat being sent
//   outputMetrics: the name(s) of the metrics being send to the
//       underlying stats
//   offset: start time is current time truncated to previous second
//       plus this offset
//   step: after each input value, time is incremented by this much
//   inputValues: all input values for the input metric, in order
//   outputValues: an array of an array of output values for each
//       metric (the first index represents each successive latch; the
//       second index corresponds to the indicies of outputMetrics)
//   setTimestampTags: controls whether TimestampTags are set
func runSimpleLatchTest(
	t *testing.T,
	inputMetric string,
	outputMetrics []string,
	offset time.Duration,
	step time.Duration,
	inputValues []float64,
	outputValues [][]float64,
	isGauge bool,
	setTimestampTags bool,
) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	underlying := newMockXstatsSender(ctrl)

	start := time.Now().Truncate(time.Second).Add(offset)

	for n, v := range outputValues {
		ts := start.Truncate(time.Second).Add(time.Duration(n) * time.Second)
		tags := []interface{}{
			fmt.Sprintf("%s=%d", TimestampTag, tbntime.ToUnixMilli(ts)),
		}

		for i, m := range outputMetrics {
			if isGauge {
				underlying.EXPECT().Gauge(m, v[i], tags...)
			} else {
				underlying.EXPECT().Count(m, v[i], tags...)
			}
		}

		underlying.EXPECT().Gauge(
			"latched_at",
			float64(ts.Unix()),
			tags...,
		)
	}

	tbntime.WithTimeAt(start, func(tc tbntime.ControlledSource) {
		s := newLatchingSender(
			underlying,
			testCleaner,
			latchWindow(time.Second),
			latchBuckets(0.001, 10),
			timeSource(tc),
		)

		for _, v := range inputValues {
			tags := make([]string, 0, 1)
			if setTimestampTags {
				tag := fmt.Sprintf(
					"%s=%d",
					TimestampTag,
					tbntime.ToUnixMilli(tc.Now()),
				)
				tags = append(tags, tag)
			}

			if isGauge {
				s.Gauge(inputMetric, v, tags...)
			} else {
				s.Count(inputMetric, v, tags...)
			}
			tc.Advance(step)
		}

		assert.Nil(t, s.(io.Closer).Close())
	})
}

func TestLatchingSenderLatchesCounters(t *testing.T) {
	runSimpleLatchTest(
		t,
		"c1",
		[]string{"c1"},
		100*time.Millisecond,
		500*time.Millisecond,
		[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[][]float64{{3}, {7}, {11}, {15}, {19}},
		false,
		true,
	)

	runSimpleLatchTest(
		t,
		"c1",
		[]string{"c1"},
		100*time.Millisecond,
		500*time.Millisecond,
		[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[][]float64{{3}, {7}, {11}, {15}, {19}},
		false,
		false,
	)
}

func TestLatchingSenderLatchesGauges(t *testing.T) {
	runSimpleLatchTest(
		t,
		"g1",
		[]string{"g1"},
		100*time.Millisecond,
		500*time.Millisecond,
		[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[][]float64{{2}, {4}, {6}, {8}, {10}},
		true,
		true,
	)
}

// Params:
//   t -- testing.T
//   inputMetric: the name of the stat being sent
//   outputMetric: the name of the stats being sent to the
//       underlying Stats
//   offset: start time is current time truncated to previous second
//       plus this offset
//   step: after each input value, time is incremented by this much
//   inputValues: all input values for the input stats, in order
//   outputHistograms: an array output histograms
func runHistogramLatchTest(
	t *testing.T,
	inputMetric string,
	offset time.Duration,
	step time.Duration,
	inputValues []float64,
	outputHistograms []*testHistogram,
	useLatchableSender bool,
) {
	runHistogramLatchTestBase(
		t,
		inputMetric,
		offset,
		step,
		inputValues,
		outputHistograms,
		useLatchableSender,
		func(s xstatsSender, name string, v float64, tags ...string) {
			s.Histogram(name, v, tags...)
		},
	)
}

// Params:
//   t -- testing.T
//   inputMetric: the name of the stat being sent
//   outputMetric: the name of the stats being sent to the
//       underlying Stats
//   offset: start time is current time truncated to previous second
//       plus this offset
//   step: after each input value, time is incremented by this much
//   inputValues: all input values for the input stats, in order
//   outputHistograms: an array output histograms
func runTimingLatchTest(
	t *testing.T,
	inputMetric string,
	offset time.Duration,
	step time.Duration,
	inputValues []float64,
	outputHistograms []*testHistogram,
	useLatchableSender bool,
) {
	runHistogramLatchTestBase(
		t,
		inputMetric,
		offset,
		step,
		inputValues,
		outputHistograms,
		useLatchableSender,
		func(s xstatsSender, name string, v float64, tags ...string) {
			d := time.Duration(v * float64(time.Second))
			s.Timing(name, d, tags...)
		},
	)
}

func runHistogramLatchTestBase(
	t *testing.T,
	stat string,
	offset time.Duration,
	step time.Duration,
	inputValues []float64,
	outputHistograms []*testHistogram,
	useLatchableSender bool,
	f func(xstatsSender, string, float64, ...string),
) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Second).Add(offset)

	var underlying xstatsSender

	if useLatchableSender {
		mock := newMockLatchableSender(ctrl)

		for n, h := range outputHistograms {
			ts := start.Truncate(time.Second).Add(time.Duration(n) * time.Second)

			tags := []interface{}{
				fmt.Sprintf("%s=%d", TimestampTag, tbntime.ToUnixMilli(ts)),
			}

			latchedHisto := LatchedHistogram{
				BaseValue: DefaultHistogramBaseValue,
				Buckets:   h.buckets,
				Count:     h.count,
				Sum:       h.sum,
				Min:       h.min,
				Max:       h.max,
			}

			mock.EXPECT().LatchedHistogram(stat, latchedHisto, tags...)
			mock.EXPECT().Gauge("latched_at", float64(ts.Unix()), tags...)
		}

		underlying = mock
	} else {
		mock := newMockXstatsSender(ctrl)

		for n, h := range outputHistograms {
			ts := start.Truncate(time.Second).Add(time.Duration(n) * time.Second)

			tags := []interface{}{
				fmt.Sprintf("%s=%d", TimestampTag, tbntime.ToUnixMilli(ts)),
			}

			accum := DefaultHistogramBaseValue
			for _, b := range h.buckets {
				name := fmt.Sprintf("%s.%g", stat, accum)
				mock.EXPECT().Count(name, float64(b), tags...)
				accum *= 2.0
			}

			mock.EXPECT().Count(fmt.Sprintf("%s.count", stat), float64(h.count), tags...)
			mock.EXPECT().Count(fmt.Sprintf("%s.sum", stat), float64(h.sum), tags...)
			mock.EXPECT().Gauge(fmt.Sprintf("%s.min", stat), float64(h.min), tags...)
			mock.EXPECT().Gauge(fmt.Sprintf("%s.max", stat), float64(h.max), tags...)
			mock.EXPECT().Gauge("latched_at", float64(ts.Unix()), tags...)
		}

		underlying = mock
	}

	tbntime.WithTimeAt(start, func(tc tbntime.ControlledSource) {
		s := newLatchingSender(
			underlying,
			testCleaner,
			latchWindow(time.Second),
			latchBuckets(0.001, 10),
			timeSource(tc),
		)

		for _, v := range inputValues {
			tags := []string{
				fmt.Sprintf("%s=%d", TimestampTag, tbntime.ToUnixMilli(tc.Now())),
			}

			f(s, stat, v, tags...)
			tc.Advance(step)
		}

		assert.Nil(t, s.(io.Closer).Close())
	})
}

func TestLatchingSenderLatchesHistograms(t *testing.T) {
	runHistogramLatchTest(
		t,
		"h1",
		100*time.Millisecond,
		250*time.Millisecond,
		measurements,
		histograms,
		false,
	)
}
func TestLatchingSenderLatchesHistogramsToLatchableSender(t *testing.T) {
	runHistogramLatchTest(
		t,
		"h1_latchable",
		100*time.Millisecond,
		250*time.Millisecond,
		measurements,
		histograms,
		true,
	)
}

func TestLatchingSenderLatchesTimingsAsHistograms(t *testing.T) {
	runTimingLatchTest(
		t,
		"t1",
		100*time.Millisecond,
		250*time.Millisecond,
		measurements,
		histograms,
		false,
	)
}

func TestLatchingSenderLatchesTimingsAsHistogramsToLatchableSender(t *testing.T) {
	runTimingLatchTest(
		t,
		"t1_latchable",
		100*time.Millisecond,
		250*time.Millisecond,
		measurements,
		histograms,
		true,
	)
}

func TestLatchedTags(t *testing.T) {
	testCases := []struct {
		tags         []string
		expectedTags []string
		expectedTime *time.Time
	}{
		{
			tags:         []string{},
			expectedTags: []string{},
		},
		{
			tags:         []string{TimestampTag + "=1500000000000"},
			expectedTags: []string{},
			expectedTime: ptr.Time(tbntime.FromUnixMilli(1500000000000)),
		},
		{
			tags: []string{
				"XYZ=123",
				TimestampTag + "=1500000000000",
				"ABC=456",
			},
			expectedTags: []string{"ABC=456", "XYZ=123"},
			expectedTime: ptr.Time(tbntime.FromUnixMilli(1500000000000)),
		},
		{
			tags:         []string{"XYZ=123", "ABC=456"},
			expectedTags: []string{"ABC=456", "XYZ=123"},
		},
		{
			tags: []string{
				"XYZ=123",
				TimestampTag + "=nope",
				"ABC=456",
			},
			expectedTags: []string{
				"ABC=456",
				"XYZ=123",
				TimestampTag + "=nope",
			},
		},
	}

	for i, tc := range testCases {
		assert.Group(
			fmt.Sprintf("test case %d of %d", i+1, len(testCases)),
			t,
			func(g *assert.G) {
				ctrl := gomock.NewController(assert.Tracing(t))
				defer ctrl.Finish()

				underlying := newMockXstatsSender(ctrl)
				s := newLatchingSender(
					underlying,
					testCleaner,
					latchWindow(time.Second),
					latchBuckets(0.001, 10),
				)
				sImpl := s.(*latchingSender)
				gotTags, gotTime := sImpl.latchedTags(tc.tags)
				assert.ArrayEqual(t, gotTags, tc.expectedTags)
				assert.DeepEqual(t, gotTime, tc.expectedTime)
			},
		)
	}
}

func TestLatchingSenderLatchesOverMultipleMetrics(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	underlying := newMockXstatsSender(ctrl)

	start := time.Now().Truncate(time.Second).Add(100 * time.Millisecond)

	for n := 0; n < 5; n++ {
		ts := start.Truncate(time.Second).Add(time.Duration(n) * time.Second)

		tsTag := fmt.Sprintf("%s=%d", TimestampTag, tbntime.ToUnixMilli(ts))

		underlying.EXPECT().Count("c1", float64(4*n+3), tsTag)
		underlying.EXPECT().Count("c2", float64(4*n+7), tsTag)
		underlying.EXPECT().Gauge("g", float64(2*n+1), "a=1", tsTag)
		underlying.EXPECT().Gauge("g", float64(2*n+4), "a=2", tsTag)
		underlying.EXPECT().Gauge("latched_at", float64(ts.Unix()), tsTag)
	}

	tbntime.WithTimeAt(start, func(tc tbntime.ControlledSource) {
		s := newLatchingSender(
			underlying,
			testCleaner,
			latchWindow(time.Second),
			latchBuckets(0.001, 10),
			timeSource(tc),
		)

		for n := 0; n < 10; n++ {
			tsTag := fmt.Sprintf(
				"%s=%d",
				TimestampTag,
				tbntime.ToUnixMilli(tc.Now()),
			)

			s.Count("c1", float64(n+1), tsTag)
			s.Count("c2", float64(n+3), tsTag)
			s.Gauge("g", float64(n), tsTag, "a=1")
			s.Gauge("g", float64(n+3), tsTag, "a=2")

			tc.Advance(500 * time.Millisecond)
		}

		assert.Nil(t, s.(io.Closer).Close())
	})
}
