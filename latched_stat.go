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

type LatchedHistogram struct {
	BaseValue float64
	Buckets   []int64
	Count     int64
	Sum       float64
	Min       float64
	Max       float64
}

// counter handles counts over the latch window. All values added are
// summed and returned as a single stat.
type counter struct {
	stat  string
	value int64
	tags  []string
}

func (c *counter) add(v int64) { c.value += v }

// gauge handles gauges over the latch window. The last value added is
// returned as a single stat.
type gauge struct {
	stat  string
	value float64
	tags  []string
}

func (g *gauge) set(v float64) { g.value = v }

// histogram buckets added values into bins and produces a stat for
// each bucket, the total count, the total sum, and the minimum and
// maximum values.
type histogram struct {
	stat    string
	tags    []string
	buckets []int64
	count   int64
	sum     float64
	min     float64
	max     float64
}

func (h *histogram) add(v, baseValue float64) {
	accum := baseValue
	idx := 0
	n := len(h.buckets)

	for v > accum && idx < n {
		idx++
		accum *= 2.0
	}

	if idx < n {
		h.buckets[idx]++
	}

	h.count++
	h.sum += v
	if h.count == 1 {
		h.min = v
		h.max = v
	} else {
		if v < h.min {
			h.min = v
		}
		if v > h.max {
			h.max = v
		}
	}
}

func (h *histogram) latch(baseValue float64) LatchedHistogram {
	return LatchedHistogram{
		BaseValue: baseValue,
		Buckets:   h.buckets,
		Count:     h.count,
		Sum:       h.sum,
		Min:       h.min,
		Max:       h.max,
	}
}
