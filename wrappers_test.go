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
	"testing"
	"time"

	"github.com/turbinelabs/test/assert"
)

func TestNewRecordingStatsGauge(t *testing.T) {
	ch := make(chan Recorded, 10)

	s := NewRecordingStats(ch)
	defer func() {
		assert.Nil(t, s.Close())
	}()

	abTag := NewKVTag("a", "b")
	xyTag := NewKVTag("x", "y")

	s.Gauge("g", 1.1)
	s.Gauge("g", 1.2, abTag)
	s.AddTags(xyTag)
	s.Gauge("g", 1.3, abTag)

	s = NewRecordingStats(ch)
	s.Gauge("g", 1.4)
	s = s.Scope("i", "j")
	s.Gauge("g", 1.5)
	s.Gauge("g", 1.6, abTag)
	s.AddTags(xyTag)
	s.Gauge("g", 1.7, abTag)

	assert.DeepEqual(t, <-ch, Recorded{
		Method: "gauge",
		Metric: "g",
		Value:  1.1,
	})
	assert.DeepEqual(t, <-ch, Recorded{
		Method: "gauge",
		Metric: "g",
		Value:  1.2,
		Tags:   []Tag{abTag},
	})
	assert.DeepEqual(t, <-ch, Recorded{
		Method: "gauge",
		Metric: "g",
		Value:  1.3,
		Tags:   []Tag{xyTag, abTag},
	})
	assert.DeepEqual(t, <-ch, Recorded{
		Method: "gauge",
		Metric: "g",
		Value:  1.4,
	})
	assert.DeepEqual(t, <-ch, Recorded{
		Scope:  "i.j",
		Method: "gauge",
		Metric: "g",
		Value:  1.5,
	})
	assert.DeepEqual(t, <-ch, Recorded{
		Scope:  "i.j",
		Method: "gauge",
		Metric: "g",
		Value:  1.6,
		Tags:   []Tag{abTag},
	})
	assert.DeepEqual(t, <-ch, Recorded{
		Scope:  "i.j",
		Method: "gauge",
		Metric: "g",
		Value:  1.7,
		Tags:   []Tag{xyTag, abTag},
	})

	assert.ChannelEmpty(t, ch)
}

func TestNewRecordingStatsCount(t *testing.T) {
	ch := make(chan Recorded, 10)

	abTag := NewKVTag("a", "b")
	xyTag := NewKVTag("x", "y")

	s := NewRecordingStats(ch)
	defer func() {
		assert.Nil(t, s.Close())
	}()

	s.AddTags(xyTag)
	s = s.Scope("i", "j")
	s = s.Scope("k")
	s.Count("c", 100.0, abTag, abTag)

	assert.DeepEqual(t, <-ch, Recorded{
		Scope:  "i.j.k",
		Method: "count",
		Metric: "c",
		Value:  100.0,
		Tags:   []Tag{xyTag, abTag, abTag},
	})

	assert.ChannelEmpty(t, ch)
}

func TestNewRecordingStatsHistogram(t *testing.T) {
	ch := make(chan Recorded, 10)

	abTag := NewKVTag("a", "b")
	xyTag := NewKVTag("x", "y")

	s := NewRecordingStats(ch)
	defer func() {
		assert.Nil(t, s.Close())
	}()

	s.AddTags(xyTag)
	s = s.Scope("i", "j")
	s = s.Scope("k")
	s.Histogram("h", 200.0, abTag, abTag)

	assert.DeepEqual(t, <-ch, Recorded{
		Scope:  "i.j.k",
		Method: "histogram",
		Metric: "h",
		Value:  200.0,
		Tags:   []Tag{xyTag, abTag, abTag},
	})

	assert.ChannelEmpty(t, ch)
}

func TestNewRecordingStatsTiming(t *testing.T) {
	ch := make(chan Recorded, 10)

	abTag := NewKVTag("a", "b")
	xyTag := NewKVTag("x", "y")

	s := NewRecordingStats(ch)
	defer func() {
		assert.Nil(t, s.Close())
	}()

	s.AddTags(xyTag)
	s = s.Scope("i", "j")
	s = s.Scope("k")
	s.Timing("t", 10*time.Second, abTag, abTag)

	assert.DeepEqual(t, <-ch, Recorded{
		Scope:  "i.j.k",
		Method: "timing",
		Metric: "t",
		Timing: 10 * time.Second,
		Tags:   []Tag{xyTag, abTag, abTag},
	})

	assert.ChannelEmpty(t, ch)
}
