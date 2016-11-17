package stats

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

import (
	"time"

	"github.com/turbinelabs/statsd"
)

// StatsPayload is the payload of a stats update call.
type StatsPayload struct {
	Source string `json:"source"`
	Stats  []Stat `json:"stats"`
}

type Stat struct {
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Timestamp int64             `json:"timestamp"` // microseconds since the Unix epoch, UTC
	Tags      map[string]string `json:"tags,omitempty"`
}

// Result is a JSON-encodable struct that encapsulates the result of
// forwarding metrics.
type Result struct {
	NumAccepted int `json:"numAccepted"`
}

// Stats represents a simple interface for forwarding arbitrary stats.
// For compatibility, it is a subset of the functionality in
// github.com/turbinelabs/statsd.Stats
type Stats interface {
	Inc(string, int64) error
	Gauge(string, int64) error
	TimingDuration(string, time.Duration) error
}

var _ Stats = &struct{ statsd.Stats }{}

// Creates a do-nothing Stats.
func NewNoopStats() Stats {
	return &noopStats{}
}

type noopStats struct{}

func (_ *noopStats) Inc(_ string, _ int64) error                    { return nil }
func (_ *noopStats) Gauge(_ string, _ int64) error                  { return nil }
func (_ *noopStats) TimingDuration(_ string, _ time.Duration) error { return nil }

// Creates a Stats implementation that forwards all calls to an
// underlying Stats using goroutines. All methods always return nil.
func NewAsyncStats(s Stats) Stats {
	return &asyncStats{underlying: s}
}

type asyncStats struct {
	underlying Stats
}

func (a *asyncStats) Inc(name string, value int64) error {
	go a.underlying.Inc(name, value)
	return nil
}

func (a *asyncStats) Gauge(name string, value int64) error {
	go a.underlying.Gauge(name, value)
	return nil
}

func (a *asyncStats) TimingDuration(name string, value time.Duration) error {
	go a.underlying.TimingDuration(name, value)
	return nil
}
