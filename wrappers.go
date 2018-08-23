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
	"strings"
	"time"
)

// NewNoopStats returns a Stats implementation that does nothing.
func NewNoopStats() Stats { return &noop{} }

type noop struct{}

func (n *noop) Gauge(_ string, _ float64, _ ...Tag)        {}
func (n *noop) Count(_ string, _ float64, _ ...Tag)        {}
func (n *noop) Histogram(_ string, _ float64, _ ...Tag)    {}
func (n *noop) Timing(_ string, _ time.Duration, _ ...Tag) {}
func (n *noop) Event(_ string, _ ...Field)                 {}
func (n *noop) AddTags(_ ...Tag)                           {}
func (n *noop) Scope(_ string, _ ...string) Stats          { return n }
func (n *noop) Close() error                               { return nil }

var _ Stats = &noop{}

// NewAsyncStats creates a Stats implementation that forwards all
// measurement calls to an underlying Stats using goroutines. Scoped
// Stats instances created by this Stats will also be asynchronous.
func NewAsyncStats(s Stats) Stats {
	return &async{s}
}

type async struct {
	Stats
}

func (a *async) Gauge(s string, v float64, t ...Tag)        { go a.Stats.Gauge(s, v, t...) }
func (a *async) Count(s string, v float64, t ...Tag)        { go a.Stats.Count(s, v, t...) }
func (a *async) Histogram(s string, v float64, t ...Tag)    { go a.Stats.Histogram(s, v, t...) }
func (a *async) Timing(s string, d time.Duration, t ...Tag) { go a.Stats.Timing(s, d, t...) }

func (a *async) Scope(scope string, scopes ...string) Stats {
	return &async{a.Stats.Scope(scope, scopes...)}
}

// NewRecordingStats returns a Stats implementation that records calls on the given
// channel.
func NewRecordingStats(ch chan<- Recorded) Stats {
	return &recorder{ch: ch}
}

// Recorded represents a stats call recorded by a Stats object returned from
// NewRecordingStats.
type Recorded struct {
	Method string
	Scope  string
	Metric string
	Value  float64
	Timing time.Duration
	Tags   []Tag
}

type recorder struct {
	ch chan<- Recorded

	scope string
	tags  []Tag
}

func (r *recorder) recV(method, metric string, value float64, tags []Tag) {
	r.ch <- Recorded{
		Method: method,
		Scope:  r.scope,
		Metric: metric,
		Value:  value,
		Tags:   append(r.tags, tags...),
	}
}

func (r *recorder) recT(method, metric string, timing time.Duration, tags []Tag) {
	r.ch <- Recorded{
		Method: method,
		Scope:  r.scope,
		Metric: metric,
		Timing: timing,
		Tags:   append(r.tags, tags...),
	}
}

func (r *recorder) Gauge(m string, v float64, t ...Tag)        { r.recV("gauge", m, v, t) }
func (r *recorder) Count(m string, v float64, t ...Tag)        { r.recV("count", m, v, t) }
func (r *recorder) Histogram(m string, v float64, t ...Tag)    { r.recV("histogram", m, v, t) }
func (r *recorder) Timing(m string, d time.Duration, t ...Tag) { r.recT("timing", m, d, t) }
func (r *recorder) Event(m string, f ...Field)                 {}
func (r *recorder) AddTags(t ...Tag)                           { r.tags = append(r.tags, t...) }
func (r *recorder) Close() error                               { close(r.ch); return nil }

func (r *recorder) Scope(scope string, scopes ...string) Stats {
	final := make([]string, 0, len(scopes)+2)
	if r.scope != "" {
		final = append(final, r.scope)
	}
	final = append(final, scope)
	final = append(final, scopes...)

	return &recorder{
		ch:    r.ch,
		scope: strings.Join(final, "."),
		tags:  r.tags,
	}
}
