package stats

//go:generate $TBN_HOME/scripts/mockgen_internal.sh -type latchableSender -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE -aux_files xstats=$TBN_HOME/vendor/github.com/rs/xstats/sender.go

import (
	"crypto/md5"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/rs/xstats"

	"github.com/turbinelabs/nonstdlib/ptr"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
	tbntime "github.com/turbinelabs/nonstdlib/time"
)

const (
	// DefaultLatchWindow specifies the default window over which stats are
	// aggregated.
	DefaultLatchWindow = 1 * time.Minute

	// DefaultHistogramNumBuckets specifies the number buckets used when aggregating
	// timing/histogram values.
	DefaultHistogramNumBuckets = 20

	// DefaultHistogramBaseValue controls the upper bound of the lower bucket used
	// when aggregating timing/histogram values.
	DefaultHistogramBaseValue = 0.001 // 1 millisecond in fractional seconds

	// LatchedAtMetric is the name of the synthetic metric used to report when the
	// latch occurred.
	LatchedAtMetric = "latched_at"
)

// latchableSender provides an interface that allows client-aggregated
// histogram data to be provided to the sender in a single call.
type latchableSender interface {
	xstats.Sender

	// LatchedHistogram accepts a named, client-aggregated batch
	// of histogram data and associated tags.
	LatchedHistogram(string, LatchedHistogram, ...string)
}

// newLatchingSender constructs an xstats Sender instance that latches
// stats. For each counter, it periodically emits a single value
// containing the count for the entire period. For each gauge, it
// emits the last value seen during the period. For timings and
// histogram values it computes a bucketed histogram over the window
// and emits the bucketed values, a total sum, a total count, a
// minimum value, and a maximum value. Stats are considered equivalent
// when they have the same type, name, and tags.
//
// The latching Sender implementation uses TimestampTag to allow
// provision of explicit timestamps. Timestamps are expected to be
// stringified milliseconds since the Unix epoch. Note that the
// underlying sender may not be able to store data with the given
// timestamp (and may pass it on as a tag).
//
// At the end of each latching period, a gauge named "latched_at" is
// emitted with the latch time in seconds.
func newLatchingSender(
	underlying xstatsSender,
	c cleaner,
	options ...latchingSenderOption,
) xstatsSender {
	s := &latchingSender{
		lock:                &sync.Mutex{},
		underlying:          underlying,
		cleaner:             c,
		latchWindow:         DefaultLatchWindow,
		numHistogramBuckets: DefaultHistogramNumBuckets,
		baseHistogramValue:  DefaultHistogramBaseValue,
		timeSource:          tbntime.NewSource(),
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

// latchingSenderOption is an option for configuring latching Sender
// instances created via newLatchingSender.
type latchingSenderOption func(*latchingSender)

// latchWindow controls the time span of each latched set of
// stats. Stats timestamps are truncated to this time.Duration via
// time.Time's Truncate function.
func latchWindow(d time.Duration) latchingSenderOption {
	return func(f *latchingSender) {
		f.latchWindow = d
	}
}

// latchBuckets controls generation of histograms. Each bucket of the histogram has
// an upper bound of 2^N * baseValue where N is in the range [0, numBuckets).
func latchBuckets(baseValue float64, numBuckets int) latchingSenderOption {
	return func(f *latchingSender) {
		f.baseHistogramValue = baseValue
		f.numHistogramBuckets = numBuckets
	}
}

// timeSource sets the tbntime.Source used to retrieve the current time for
// testing purposes.
func timeSource(src tbntime.Source) latchingSenderOption {
	return func(f *latchingSender) {
		f.timeSource = src
	}
}

type latchingSender struct {
	lock                *sync.Mutex
	underlying          xstatsSender
	cleaner             cleaner
	latchWindow         time.Duration
	numHistogramBuckets int
	baseHistogramValue  float64
	timeSource          tbntime.Source

	latchStart time.Time
	counters   map[string]*counter
	gauges     map[string]*gauge
	histograms map[string]*histogram
}

func (s *latchingSender) Count(stat string, count float64, tags ...string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	statID, latchedTags := s.prepareLatch(stat, tags)

	c := s.counters[statID]
	if c == nil {
		c = &counter{
			stat: stat,
			tags: latchedTags,
		}
		s.counters[statID] = c
	}

	c.add(int64(count))
}

func (s *latchingSender) Gauge(stat string, value float64, tags ...string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	statID, latchedTags := s.prepareLatch(stat, tags)

	g := s.gauges[statID]
	if g == nil {
		g = &gauge{
			stat: stat,
			tags: latchedTags,
		}
		s.gauges[statID] = g
	}

	g.set(value)
}

func (s *latchingSender) Histogram(stat string, value float64, tags ...string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	statID, latchedTags := s.prepareLatch(stat, tags)

	h := s.histograms[statID]
	if h == nil {
		h = &histogram{
			stat:    stat,
			tags:    latchedTags,
			buckets: make([]int64, s.numHistogramBuckets),
		}
		s.histograms[statID] = h
	}

	h.add(value, s.baseHistogramValue)
}

func (s *latchingSender) Timing(stat string, value time.Duration, tags ...string) {
	s.Histogram(stat, value.Seconds(), tags...)
}

func (s *latchingSender) Close() error {
	s.completeLatch(s.timeSource.Now())
	return xstats.CloseSender(s.underlying)
}

func (s *latchingSender) prepareLatch(stat string, tags []string) (string, []string) {
	var ts *time.Time
	tags, ts = s.latchedTags(tags)
	if ts == nil {
		ts = ptr.Time(s.timeSource.Now())
	}

	newStart := ts.Truncate(s.latchWindow)
	if !newStart.Equal(s.latchStart) {
		if s.latchStart.IsZero() {
			s.completeLatch(newStart)
		} else {
			nextStart := s.latchStart.Add(s.latchWindow)
			for nextStart.Before(newStart) || nextStart.Equal(newStart) {
				s.completeLatch(nextStart)
				nextStart = nextStart.Add(s.latchWindow)
			}
		}
	}

	hasher := md5.New()
	hasher.Write([]byte(stat))

	for _, tag := range tags {
		hasher.Write([]byte{'|'})
		hasher.Write([]byte(tag))
	}

	id := string(hasher.Sum(make([]byte, 0, md5.Size)))
	return id, tags
}

func (s *latchingSender) stat(stat, suffix string) string {
	return fmt.Sprintf("%s%s%s", stat, s.cleaner.scopeDelim, suffix)
}

// Complete the current latch window by:
// 1. Computing results for all stats in the window,
// 2. Generating a latched_at gauge, and
// 3. Sending all stats metrics via the underlying Stats. Initializes
//    the next latch window with the given time.
func (s *latchingSender) completeLatch(nextLatchStart time.Time) {
	sent := 0
	for _, c := range s.counters {
		s.underlying.Count(c.stat, float64(c.value), s.tagsWithTimestamp(c.tags)...)
		sent++
	}

	for _, g := range s.gauges {
		s.underlying.Gauge(g.stat, g.value, s.tagsWithTimestamp(g.tags)...)
		sent++
	}

	if latchableSender, ok := s.underlying.(latchableSender); ok {
		for _, h := range s.histograms {
			tags := s.tagsWithTimestamp(h.tags)
			latched := h.latch(s.baseHistogramValue)

			latchableSender.LatchedHistogram(h.stat, latched, tags...)
			sent++
		}
	} else {
		for _, h := range s.histograms {
			tags := s.tagsWithTimestamp(h.tags)

			baseValue := s.baseHistogramValue
			total := int64(0)
			for _, c := range h.buckets {
				v := float64(c)
				s.underlying.Count(
					s.stat(h.stat, strconv.FormatFloat(baseValue, 'g', -1, 64)),
					v,
					tags...,
				)
				baseValue = baseValue * 2.0
				total += c
			}

			s.underlying.Count(s.stat(h.stat, "count"), float64(h.count), tags...)
			s.underlying.Count(s.stat(h.stat, "sum"), h.sum, tags...)
			s.underlying.Gauge(s.stat(h.stat, "min"), h.min, tags...)
			s.underlying.Gauge(s.stat(h.stat, "max"), h.max, tags...)
			sent++
		}
	}

	if sent > 0 {
		s.underlying.Gauge(LatchedAtMetric, float64(s.latchStart.Unix()), s.tagsWithTimestamp(nil)...)
	}

	s.latchStart = nextLatchStart
	s.counters = map[string]*counter{}
	s.gauges = map[string]*gauge{}
	s.histograms = map[string]*histogram{}
}

func (s *latchingSender) tagsWithTimestamp(tags []string) []string {
	ts := tbntime.ToUnixMilli(s.latchStart)
	return append(
		tags,
		s.cleaner.tagToString(NewKVTag(TimestampTag, strconv.FormatInt(ts, 10))),
	)
}

func (s *latchingSender) latchedTags(tags []string) ([]string, *time.Time) {
	sort.Strings(tags)

	tsIdx := sort.Search(
		len(tags),
		func(i int) bool {
			return tags[i] >= TimestampTag
		},
	)

	if tsIdx < len(tags) {
		k, v := tbnstrings.Split2(tags[tsIdx], s.cleaner.tagDelim)
		if k == TimestampTag {
			if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
				t := tbntime.FromUnixMilli(ts)
				copy(tags[tsIdx:], tags[tsIdx+1:])
				return tags[0 : len(tags)-1], &t
			}
		}
	}

	return tags, nil
}
