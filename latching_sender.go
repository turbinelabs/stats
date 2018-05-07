/*
Copyright 2018 Turbine Labs, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stats

// TODO currently this can't be executed from the open source project
//go:generate $TBN_HOME/scripts/mockgen_internal.sh -type latchableSender -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE -aux_files xstats=vendor/github.com/rs/xstats/sender.go --write_package_comment=false

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
		latchingNodes:       map[string]*latchingNode{},
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

	latchingNodes map[string]*latchingNode
}

type latchingNode struct {
	lock *sync.Mutex

	latchStart time.Time
	counters   map[string]*counter
	gauges     map[string]*gauge
	histograms map[string]*histogram
}

func (s *latchingSender) Count(stat string, count float64, tags ...string) {
	latchingNode, statID, latchedTags := s.prepareLatch(stat, tags)
	defer latchingNode.lock.Unlock()

	c := latchingNode.counters[statID]
	if c == nil {
		c = &counter{
			stat: stat,
			tags: latchedTags,
		}
		latchingNode.counters[statID] = c
	}

	c.add(int64(count))
}

func (s *latchingSender) Gauge(stat string, value float64, tags ...string) {
	latchingNode, statID, latchedTags := s.prepareLatch(stat, tags)
	defer latchingNode.lock.Unlock()

	g := latchingNode.gauges[statID]
	if g == nil {
		g = &gauge{
			stat: stat,
			tags: latchedTags,
		}
		latchingNode.gauges[statID] = g
	}

	g.set(value)
}

func (s *latchingSender) Histogram(stat string, value float64, tags ...string) {
	latchingNode, statID, latchedTags := s.prepareLatch(stat, tags)
	defer latchingNode.lock.Unlock()

	h := latchingNode.histograms[statID]
	if h == nil {
		h = &histogram{
			stat:    stat,
			tags:    latchedTags,
			buckets: make([]int64, s.numHistogramBuckets),
		}
		latchingNode.histograms[statID] = h
	}

	h.add(value, s.baseHistogramValue)
}

func (s *latchingSender) Timing(stat string, value time.Duration, tags ...string) {
	s.Histogram(stat, value.Seconds(), tags...)
}

func (s *latchingSender) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	for nodeTag, latchingNode := range s.latchingNodes {
		latchingNode.lock.Lock()
		latchingNode.completeLatch(s.timeSource.Now(), nodeTag, s)
		latchingNode.lock.Unlock()
	}
	s.latchingNodes = nil

	return xstats.CloseSender(s.underlying)
}

func (s *latchingSender) latchingNode(nodeTag string) *latchingNode {
	s.lock.Lock()
	defer s.lock.Unlock()

	node := s.latchingNodes[nodeTag]
	if node == nil {
		node = &latchingNode{lock: &sync.Mutex{}}
		s.latchingNodes[nodeTag] = node
	}

	return node
}

func (s *latchingSender) prepareLatch(
	stat string,
	tags []string,
) (*latchingNode, string, []string) {
	var (
		nodeTag string
		ts      *time.Time
	)
	tags, nodeTag, ts = s.latchedTags(tags)
	if ts == nil {
		ts = ptr.Time(s.timeSource.Now())
	}

	hasher := md5.New()
	hasher.Write([]byte(stat))

	for _, tag := range tags {
		hasher.Write([]byte{'|'})
		hasher.Write([]byte(tag))
	}
	id := string(hasher.Sum(make([]byte, 0, md5.Size)))

	node := s.latchingNode(nodeTag)
	node.lock.Lock()

	newStart := ts.Truncate(s.latchWindow)
	if !newStart.Equal(node.latchStart) {
		if node.latchStart.IsZero() {
			node.completeLatch(newStart, nodeTag, s)
		} else {
			nextStart := node.latchStart.Add(s.latchWindow)
			for nextStart.Before(newStart) || nextStart.Equal(newStart) {
				node.completeLatch(nextStart, nodeTag, s)
				nextStart = nextStart.Add(s.latchWindow)
			}
		}
	}

	return node, id, tags
}

func (s *latchingSender) stat(stat, suffix string) string {
	return fmt.Sprintf("%s%s%s", stat, s.cleaner.scopeDelim, suffix)
}

// Complete the current latch window by:
// 1. Computing results for all stats in the window,
// 2. Generating a latched_at gauge, and
// 3. Sending all stats metrics via the underlying Stats. Initializes
//    the next latch window with the given time.
func (n *latchingNode) completeLatch(nextLatchStart time.Time, nodeTag string, s *latchingSender) {
	sent := 0
	for _, c := range n.counters {
		s.underlying.Count(c.stat, float64(c.value), n.tagsWithTimestamp(s, c.tags)...)
		sent++
	}

	for _, g := range n.gauges {
		s.underlying.Gauge(g.stat, g.value, n.tagsWithTimestamp(s, g.tags)...)
		sent++
	}

	if latchableSender, ok := s.underlying.(latchableSender); ok {
		for _, h := range n.histograms {
			tags := n.tagsWithTimestamp(s, h.tags)
			latched := h.latch(s.baseHistogramValue)

			latchableSender.LatchedHistogram(h.stat, latched, tags...)
			sent++
		}
	} else {
		for _, h := range n.histograms {
			tags := n.tagsWithTimestamp(s, h.tags)

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
		var latchedAtTags []string
		if nodeTag != "" {
			latchedAtTags = []string{fmt.Sprintf("%s=%s", NodeTag, nodeTag)}
		}
		s.underlying.Gauge(
			LatchedAtMetric,
			float64(n.latchStart.Unix()),
			n.tagsWithTimestamp(s, latchedAtTags)...,
		)
	}

	n.latchStart = nextLatchStart
	n.counters = map[string]*counter{}
	n.gauges = map[string]*gauge{}
	n.histograms = map[string]*histogram{}
}

func (n *latchingNode) tagsWithTimestamp(s *latchingSender, tags []string) []string {
	ts := tbntime.ToUnixMilli(n.latchStart)
	return append(
		tags,
		s.cleaner.tagToString(NewKVTag(TimestampTag, strconv.FormatInt(ts, 10))),
	)
}

// Returns sorted tags, with TimestampTag removed. If NodeTag is present, it's value
// is returned (but it remains in tags). If TimestampTag is present, it's value is
// returned.
func (s *latchingSender) latchedTags(tags []string) ([]string, string, *time.Time) {
	sort.Strings(tags)

	var ts *time.Time
	idx := sort.Search(
		len(tags),
		func(i int) bool {
			return tags[i] >= TimestampTag
		},
	)
	if idx < len(tags) {
		k, v := tbnstrings.Split2(tags[idx], s.cleaner.tagDelim)
		if k == TimestampTag {
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				t := tbntime.FromUnixMilli(i)
				copy(tags[idx:], tags[idx+1:])
				tags = tags[0 : len(tags)-1]
				ts = &t
			}
		}
	}

	node := ""
	idx = sort.Search(
		len(tags),
		func(i int) bool {
			return tags[i] >= NodeTag
		},
	)
	if idx < len(tags) {
		k, v := tbnstrings.Split2(tags[idx], s.cleaner.tagDelim)
		if k == NodeTag {
			node = v
		}
	}

	return tags, node, ts
}
