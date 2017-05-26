package stats

import "time"

// NewMulti returns a Stats implementation that forwards calls to multiple
// Stats backends
func NewMulti(statses ...Stats) Stats {
	return multiStats(statses)
}

type multiStats []Stats

func (ms multiStats) Gauge(stat string, value float64, tags ...Tag) {
	for _, s := range ms {
		s.Gauge(stat, value, tags...)
	}
}

func (ms multiStats) Count(stat string, count float64, tags ...Tag) {
	for _, s := range ms {
		s.Count(stat, count, tags...)
	}
}

func (ms multiStats) Histogram(stat string, value float64, tags ...Tag) {
	for _, s := range ms {
		s.Histogram(stat, value, tags...)
	}
}

func (ms multiStats) Timing(stat string, value time.Duration, tags ...Tag) {
	for _, s := range ms {
		s.Timing(stat, value, tags...)
	}
}

func (ms multiStats) AddTags(tags ...Tag) {
	for _, s := range ms {
		s.AddTags(tags...)
	}
}

func (ms multiStats) Scope(scope string, scopes ...string) Stats {
	newStatses := make([]Stats, 0, len(ms))
	for _, s := range ms {
		newStatses = append(newStatses, s.Scope(scope, scopes...))
	}
	return multiStats(newStatses)
}

func (ms multiStats) Close() error {
	var firstErr error
	for _, s := range ms {
		err := s.Close()
		if err != nil {
			firstErr = err
		}
	}
	return firstErr
}
