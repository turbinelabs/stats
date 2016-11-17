package client

import (
	"strings"
	"time"

	tbntime "github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/stats"
)

func newStats(client StatsClient, source string, scope ...string) stats.Stats {
	resolvedScope := strings.Join(scope, "/")
	return &statsT{
		client: client,
		source: source,
		scope:  resolvedScope,
	}
}

type statsT struct {
	client StatsClient
	source string
	scope  string
}

var _ stats.Stats = &statsT{}

func (s *statsT) stat(name string, value float64) error {
	if s.scope != "" {
		name = s.scope + "/" + name
	}

	payload := &stats.StatsPayload{
		Source: s.source,
		Stats: []stats.Stat{
			{
				Name:      name,
				Value:     value,
				Timestamp: tbntime.ToUnixMicro(time.Now()),
			},
		},
	}
	_, err := s.client.Forward(payload)
	return err
}

func (s *statsT) Inc(name string, v int64) error {
	return s.stat(name, float64(v))
}

func (s *statsT) Gauge(name string, v int64) error {
	return s.stat(name, float64(v))
}

func (s *statsT) TimingDuration(name string, d time.Duration) error {
	return s.stat(name, d.Seconds())
}
