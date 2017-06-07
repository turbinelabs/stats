package stats

import "time"

// NewMulti returns a Stats implementation that forwards calls to multiple
// Stats backends
func NewMulti(statses ...Stats) Stats {
	return multiStats(statses)
}

// NewRollUp creates a Stats whose scopes delegate stats back to itself. For example,
//
//     h := NewRollUp(stats)
//     scoped := h.Scope("x", "y", "z")
//     scoped.Count("c", 1.0)
//
// causes the following counters to be recorded: "c", "x.c", "x.y.c",
// and "x.y.z.c" (assuming a period is used as the scoped delimiter).
func NewRollUp(root Stats) Stats {
	return &rollUpStats{self: root}
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
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type rollUpStats struct {
	self     Stats
	parent   Stats
	children map[string]*rollUpStats
}

func (hs *rollUpStats) Gauge(stat string, value float64, tags ...Tag) {
	if hs.parent != nil {
		hs.parent.Gauge(stat, value, tags...)
	}

	hs.self.Gauge(stat, value, tags...)
}

func (hs *rollUpStats) Count(stat string, count float64, tags ...Tag) {
	if hs.parent != nil {
		hs.parent.Count(stat, count, tags...)
	}
	hs.self.Count(stat, count, tags...)
}

func (hs *rollUpStats) Histogram(stat string, value float64, tags ...Tag) {
	if hs.parent != nil {
		hs.parent.Histogram(stat, value, tags...)
	}
	hs.self.Histogram(stat, value, tags...)
}

func (hs *rollUpStats) Timing(stat string, value time.Duration, tags ...Tag) {
	if hs.parent != nil {
		hs.parent.Timing(stat, value, tags...)
	}
	hs.self.Timing(stat, value, tags...)
}

func (hs *rollUpStats) AddTags(tags ...Tag) {
	hs.self.AddTags(tags...)
}

func (hs *rollUpStats) Scope(scope string, scopes ...string) Stats {
	if hs.children == nil {
		hs.children = map[string]*rollUpStats{}
	} else if child, ok := hs.children[scope]; ok {
		if len(scopes) == 0 {
			return child
		}

		return child.Scope(scopes[0], scopes[1:]...)
	}

	child := &rollUpStats{
		parent: hs,
		self:   hs.self.Scope(scope),
	}

	hs.children[scope] = child

	if len(scopes) != 0 {
		return child.Scope(scopes[0], scopes[1:]...)
	}

	return child
}

func (hs *rollUpStats) Close() error {
	return hs.self.Close()
}
