package stats

import (
	"math"
	"strconv"
	"time"

	"github.com/turbinelabs/api/service/stats"
	tbnmath "github.com/turbinelabs/nonstdlib/math"
	"github.com/turbinelabs/nonstdlib/ptr"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
	tbntime "github.com/turbinelabs/nonstdlib/time"
)

var (
	truePtr = ptr.Bool(true)

	apiCleaner = cleaner{
		cleanStatName: identity,
		cleanTagName:  mkStrip("="),
		cleanTagValue: identity,
		tagDelim:      "=",
		scopeDelim:    "/",
	}
)

type apiSender struct {
	svc    stats.StatsService
	source string
}

func (s *apiSender) toTagMap(tags []string) (map[string]string, time.Time) {
	var (
		tagsMap map[string]string
		ts      *time.Time
	)

	if len(tags) > 0 {
		tagsMap = make(map[string]string, len(tags))
		for _, tag := range tags {
			k, v := tbnstrings.SplitFirstEqual(tag)
			if ts == nil && k == TimestampTag {
				if v, err := strconv.ParseInt(v, 10, 64); err == nil {
					ts = ptr.Time(tbntime.FromUnixMilli(v))
					continue
				}
			}
			tagsMap[k] = v
		}
	}

	if ts == nil {
		return tagsMap, time.Now()
	}

	return tagsMap, *ts
}

func (s *apiSender) Count(stat string, value float64, tags ...string) {
	tagMap, ts := s.toTagMap(tags)

	payload := &stats.Payload{
		Source: s.source,
		Stats: []stats.Stat{
			{
				Name:      stat,
				Value:     &value,
				Timestamp: tbntime.ToUnixMicro(ts),
				Tags:      tagMap,
			},
		},
	}

	s.svc.Forward(payload)
}

func (s *apiSender) Gauge(stat string, value float64, tags ...string) {
	tagMap, ts := s.toTagMap(tags)

	payload := &stats.Payload{
		Source: s.source,
		Stats: []stats.Stat{
			{
				Name:      stat,
				Value:     &value,
				IsGauge:   truePtr,
				Timestamp: tbntime.ToUnixMicro(ts),
				Tags:      tagMap,
			},
		},
	}

	s.svc.Forward(payload)
}

func (s *apiSender) Histogram(stat string, value float64, tags ...string) {
	tagMap, ts := s.toTagMap(tags)

	payload := &stats.Payload{
		Source: s.source,
		Stats: []stats.Stat{
			{
				Name:      stat,
				Value:     &value,
				Timestamp: tbntime.ToUnixMicro(ts),
				Tags:      tagMap,
			},
		},
	}

	s.svc.Forward(payload)
}

func (s *apiSender) Timing(stat string, value time.Duration, tags ...string) {
	s.Histogram(stat, value.Seconds(), tags...)
}

func (s *apiSender) LatchedHistogram(stat string, h LatchedHistogram, tags ...string) {
	tagMap, ts := s.toTagMap(tags)

	histo := &stats.Histogram{
		Buckets: make([][2]float64, len(h.Buckets)),
		Count:   h.Count,
		Sum:     h.Sum,
		Minimum: h.Min,
		Maximum: h.Max,
	}

	accum := h.BaseValue
	total := int64(0)
	p50 := math.NaN()
	p99 := math.NaN()

	for i, c := range h.Buckets {
		histo.Buckets[i][0] = accum
		histo.Buckets[i][1] = float64(c)
		total += c
		if math.IsNaN(p50) && total >= tbnmath.Round(float64(h.Count)*0.5) {
			p50 = accum
		}
		if math.IsNaN(p99) && total >= tbnmath.Round(float64(h.Count)*0.99) {
			p99 = accum
		}
		accum *= 2.0
	}

	if math.IsNaN(p50) {
		histo.P50 = 0.0
	} else {
		histo.P50 = p50
	}
	if math.IsNaN(p99) {
		histo.P99 = 0.0
	} else {
		histo.P99 = p99
	}

	payload := &stats.Payload{
		Source: s.source,
		Stats: []stats.Stat{
			{
				Name:      stat,
				Histogram: histo,
				Timestamp: tbntime.ToUnixMicro(ts),
				Tags:      tagMap,
			},
		},
	}

	s.svc.Forward(payload)
}

func (s *apiSender) Close() error {
	return s.svc.Close()
}

var _ latchableSender = &apiSender{}
