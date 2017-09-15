package stats

import (
	"strconv"
	"time"

	"github.com/turbinelabs/api/service/stats"
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
	svc    stats.StatsServiceV2
	source string
	zone   string
	proxy  string
}

func (s *apiSender) toTagMap(tags []string) (map[string]string, *string, *string, time.Time) {
	var (
		proxy        *string
		proxyVersion *string
		tagsMap      map[string]string
		ts           *time.Time
	)

	if len(tags) > 0 {
		tagsMap = make(map[string]string, len(tags))
		for _, tag := range tags {
			k, v := tbnstrings.SplitFirstEqual(tag)
			switch k {
			case ProxyTag:
				proxy = &v

			case ProxyVersionTag:
				proxyVersion = &v

			case TimestampTag:
				if tsv, err := strconv.ParseInt(v, 10, 64); err == nil {
					ts = ptr.Time(tbntime.FromUnixMilli(tsv))
				} else {
					tagsMap[k] = v
				}

			default:
				tagsMap[k] = v
			}
		}
	}

	if ts == nil {
		ts = ptr.Time(time.Now())
	}

	if proxy == nil && s.proxy != "" {
		proxy = &s.proxy
	}

	return tagsMap, proxy, proxyVersion, *ts
}

func (s *apiSender) Count(stat string, value float64, tags ...string) {
	tagMap, proxy, proxyVersion, ts := s.toTagMap(tags)

	s.svc.ForwardV2(
		&stats.PayloadV2{
			Source:       s.source,
			Zone:         s.zone,
			Proxy:        proxy,
			ProxyVersion: proxyVersion,
			Stats: []stats.StatV2{
				{
					Name:      stat,
					Count:     &value,
					Timestamp: tbntime.ToUnixMilli(ts),
					Tags:      tagMap,
				},
			},
		},
	)
}

func (s *apiSender) Gauge(stat string, value float64, tags ...string) {
	tagMap, proxy, proxyVersion, ts := s.toTagMap(tags)

	s.svc.ForwardV2(
		&stats.PayloadV2{
			Source:       s.source,
			Zone:         s.zone,
			Proxy:        proxy,
			ProxyVersion: proxyVersion,
			Stats: []stats.StatV2{
				{
					Name:      stat,
					Gauge:     &value,
					Timestamp: tbntime.ToUnixMilli(ts),
					Tags:      tagMap,
				},
			},
		},
	)
}

func (s *apiSender) Histogram(stat string, value float64, tags ...string) {
	tagMap, proxy, proxyVersion, ts := s.toTagMap(tags)

	s.svc.ForwardV2(
		&stats.PayloadV2{
			Source:       s.source,
			Zone:         s.zone,
			Proxy:        proxy,
			ProxyVersion: proxyVersion,
			Stats: []stats.StatV2{
				{
					Name:      stat,
					Gauge:     &value,
					Timestamp: tbntime.ToUnixMilli(ts),
					Tags:      tagMap,
				},
			},
		},
	)
}

func (s *apiSender) Timing(stat string, value time.Duration, tags ...string) {
	s.Histogram(stat, value.Seconds(), tags...)
}

func (s *apiSender) LatchedHistogram(stat string, h LatchedHistogram, tags ...string) {
	tagMap, proxy, proxyVersion, ts := s.toTagMap(tags)

	histo := &stats.HistogramV2{
		Buckets: make([]int64, len(h.Buckets)),
		Count:   h.Count,
		Sum:     h.Sum,
		Minimum: h.Min,
		Maximum: h.Max,
	}

	limits := make([]float64, len(h.Buckets))

	accum := h.BaseValue

	for i, c := range h.Buckets {
		limits[i] = accum
		histo.Buckets[i] = c
		accum *= 2.0
	}

	s.svc.ForwardV2(
		&stats.PayloadV2{
			Source:       s.source,
			Zone:         s.zone,
			Proxy:        proxy,
			ProxyVersion: proxyVersion,
			Limits:       map[string][]float64{stats.DefaultLimitName: limits},
			Stats: []stats.StatV2{
				{
					Name:      stat,
					Histogram: histo,
					Timestamp: tbntime.ToUnixMilli(ts),
					Tags:      tagMap,
				},
			},
		},
	)
}

func (s *apiSender) Close() error {
	return s.svc.Close()
}

var _ latchableSender = &apiSender{}
