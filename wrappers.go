package stats

import "time"

// NewNoopStats returns a Stats implementation that does nothing.
func NewNoopStats() Stats { return &noop{} }

type noop struct{}

func (n *noop) Gauge(_ string, _ float64, _ ...Tag)        {}
func (n *noop) Count(_ string, _ float64, _ ...Tag)        {}
func (n *noop) Histogram(_ string, _ float64, _ ...Tag)    {}
func (n *noop) Timing(_ string, _ time.Duration, _ ...Tag) {}
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
