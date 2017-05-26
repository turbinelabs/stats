package stats

import (
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
		cleanTagName:  identity,
		tagDelim:      "=",
		scopeDelim:    "/",
	}
)

// NewAPIStats creates a Stats that uses the given stats.StatsService
// to forward arbitrary stats with the given source.
func NewAPIStats(svc stats.StatsService, source string) Stats {
	return newFromSender(
		&apiSender{
			svc:    svc,
			source: source,
		},
		apiCleaner,
	)
}

type apiSender struct {
	svc    stats.StatsService
	source string
}

func (s *apiSender) toTagMap(tags []string) map[string]string {
	if len(tags) > 0 {
		tagsMap := make(map[string]string, len(tags))
		for _, tag := range tags {
			k, v := tbnstrings.SplitFirstEqual(tag)
			tagsMap[k] = v
		}
		return tagsMap
	}

	return nil
}

func (s *apiSender) Count(stat string, value float64, tags ...string) {
	payload := &stats.Payload{
		Source: s.source,
		Stats: []stats.Stat{
			{
				Name:      stat,
				Value:     &value,
				Timestamp: tbntime.ToUnixMicro(time.Now()),
				Tags:      s.toTagMap(tags),
			},
		},
	}

	s.svc.Forward(payload)
}

func (s *apiSender) Gauge(stat string, value float64, tags ...string) {
	payload := &stats.Payload{
		Source: s.source,
		Stats: []stats.Stat{
			{
				Name:      stat,
				Value:     &value,
				IsGauge:   truePtr,
				Timestamp: tbntime.ToUnixMicro(time.Now()),
				Tags:      s.toTagMap(tags),
			},
		},
	}

	s.svc.Forward(payload)
}

func (s *apiSender) Histogram(stat string, value float64, tags ...string) {
	payload := &stats.Payload{
		Source: s.source,
		Stats: []stats.Stat{
			{
				Name:      stat,
				Value:     &value,
				Timestamp: tbntime.ToUnixMicro(time.Now()),
				Tags:      s.toTagMap(tags),
			},
		},
	}

	s.svc.Forward(payload)
}

func (s *apiSender) Timing(stat string, value time.Duration, tags ...string) {
	s.Histogram(stat, value.Seconds(), tags...)
}

func (s *apiSender) Close() error {
	return s.svc.Close()
}
