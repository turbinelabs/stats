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

import "time"

// NewMulti returns a Stats implementation that forwards calls to
// multiple Stats backends. Nil Stats values are ignored. If no
// non-nil Stats are passed, returns the result of NewNoopStats. If
// only a single non-nil Stats is passed, it is returned.
func NewMulti(statses ...Stats) Stats {
	for i := 0; i < len(statses); {
		if statses[i] == nil {
			statses = append(statses[:i], statses[i+1:]...)
		} else {
			i++
		}
	}

	switch len(statses) {
	case 0:
		return NewNoopStats()
	case 1:
		return statses[0]
	default:
		return multiStats(statses)
	}
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

func (ms multiStats) Event(stat string, fields ...Field) {
	for _, s := range ms {
		s.Event(stat, fields...)
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

func (hs *rollUpStats) Event(stat string, fields ...Field) {
	if hs.parent != nil {
		hs.parent.Event(stat, fields...)
	}
	hs.self.Event(stat, fields...)
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
