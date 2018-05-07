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

import (
	"strconv"
	"time"

	"github.com/turbinelabs/api/service/stats"
	"github.com/turbinelabs/api/service/stats/v2"
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
	zone   string
	proxy  string
	node   string
}

type resolvedTags struct {
	tagMap       map[string]string
	node         *string
	proxy        *string
	proxyVersion *string
	zone         string
	ts           time.Time
}

func (s *apiSender) toTagMap(tags []string) resolvedTags {
	var ts *time.Time
	resolved := resolvedTags{}

	if len(tags) > 0 {
		resolved.tagMap = make(map[string]string, len(tags))
		for _, tag := range tags {
			k, v := tbnstrings.SplitFirstEqual(tag)
			switch k {
			case NodeTag:
				resolved.node = &v

			case ProxyTag:
				resolved.proxy = &v

			case ProxyVersionTag:
				resolved.proxyVersion = &v

			case TimestampTag:
				if tsv, err := strconv.ParseInt(v, 10, 64); err == nil {
					ts = ptr.Time(tbntime.FromUnixMilli(tsv))
				} else {
					resolved.tagMap[k] = v
				}

			case ZoneTag:
				resolved.zone = v

			default:
				resolved.tagMap[k] = v
			}
		}
	}

	if ts != nil {
		resolved.ts = *ts
	} else {
		resolved.ts = time.Now()
	}

	if resolved.node == nil && s.node != "" {
		resolved.node = &s.node
	}

	if resolved.proxy == nil && s.proxy != "" {
		resolved.proxy = &s.proxy
	}

	if resolved.zone == "" && s.zone != "" {
		resolved.zone = s.zone
	}

	return resolved
}

func (s *apiSender) Count(stat string, value float64, tags ...string) {
	resolvedTags := s.toTagMap(tags)

	s.svc.ForwardV2(
		&stats.Payload{
			Source:       s.source,
			Node:         resolvedTags.node,
			Zone:         resolvedTags.zone,
			Proxy:        resolvedTags.proxy,
			ProxyVersion: resolvedTags.proxyVersion,
			Stats: []stats.Stat{
				{
					Name:      stat,
					Count:     &value,
					Timestamp: tbntime.ToUnixMilli(resolvedTags.ts),
					Tags:      resolvedTags.tagMap,
				},
			},
		},
	)
}

func (s *apiSender) Gauge(stat string, value float64, tags ...string) {
	resolvedTags := s.toTagMap(tags)

	s.svc.ForwardV2(
		&stats.Payload{
			Source:       s.source,
			Node:         resolvedTags.node,
			Zone:         resolvedTags.zone,
			Proxy:        resolvedTags.proxy,
			ProxyVersion: resolvedTags.proxyVersion,
			Stats: []stats.Stat{
				{
					Name:      stat,
					Gauge:     &value,
					Timestamp: tbntime.ToUnixMilli(resolvedTags.ts),
					Tags:      resolvedTags.tagMap,
				},
			},
		},
	)
}

func (s *apiSender) Histogram(stat string, value float64, tags ...string) {
	resolvedTags := s.toTagMap(tags)

	s.svc.ForwardV2(
		&stats.Payload{
			Source:       s.source,
			Node:         resolvedTags.node,
			Zone:         resolvedTags.zone,
			Proxy:        resolvedTags.proxy,
			ProxyVersion: resolvedTags.proxyVersion,
			Stats: []stats.Stat{
				{
					Name:      stat,
					Gauge:     &value,
					Timestamp: tbntime.ToUnixMilli(resolvedTags.ts),
					Tags:      resolvedTags.tagMap,
				},
			},
		},
	)
}

func (s *apiSender) Timing(stat string, value time.Duration, tags ...string) {
	s.Histogram(stat, value.Seconds(), tags...)
}

func (s *apiSender) LatchedHistogram(stat string, h LatchedHistogram, tags ...string) {
	resolvedTags := s.toTagMap(tags)

	histo := &stats.Histogram{
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
		&stats.Payload{
			Source:       s.source,
			Node:         resolvedTags.node,
			Zone:         resolvedTags.zone,
			Proxy:        resolvedTags.proxy,
			ProxyVersion: resolvedTags.proxyVersion,
			Limits:       map[string][]float64{v2.DefaultLimitName: limits},
			Stats: []stats.Stat{
				{
					Name:      stat,
					Histogram: histo,
					Timestamp: tbntime.ToUnixMilli(resolvedTags.ts),
					Tags:      resolvedTags.tagMap,
				},
			},
		},
	)
}

func (s *apiSender) Close() error {
	return s.svc.Close()
}

var _ latchableSender = &apiSender{}
