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

	"github.com/turbinelabs/test/assert"
)

func TestCounter(t *testing.T) {
	tags := []string{"abc=def"}

	c := &counter{"abc", 0, tags}
	c.add(1)
	assert.Equal(t, c.value, int64(1))
	assert.Equal(t, c.stat, "abc")
	assert.ArrayEqual(t, c.tags, tags)

	c = &counter{"abc", 100, tags}
	c.add(50)
	assert.Equal(t, c.value, int64(150))

}

func TestGauge(t *testing.T) {
	tags := []string{"abc=def"}

	g := &gauge{"abc", 0.0, tags}
	g.set(1.0)
	assert.Equal(t, g.value, 1.0)
	assert.Equal(t, g.stat, "abc")
	assert.ArrayEqual(t, g.tags, tags)

	g = &gauge{"abc", 100.0, tags}
	g.set(50.0)
	assert.Equal(t, g.value, 50.0)
	g.set(150.0)
	assert.Equal(t, g.value, 150.0)
}

func TestHistogram(t *testing.T) {
	tags := []string{"abc=def"}

	baseValue := 1.0

	h := &histogram{"abc", tags, make([]int64, 4), 0, 0, 0, 0}

	h.add(3.0, baseValue)
	assert.Equal(t, h.stat, "abc")
	assert.ArrayEqual(t, h.tags, tags)
	assert.ArrayEqual(t, h.buckets, []int64{0, 0, 1, 0})
	assert.Equal(t, h.count, int64(1))
	assert.Equal(t, h.sum, 3.0)
	assert.Equal(t, h.min, 3.0)
	assert.Equal(t, h.max, 3.0)

	h.add(5.0, baseValue)
	assert.ArrayEqual(t, h.buckets, []int64{0, 0, 1, 1})
	assert.Equal(t, h.count, int64(2))
	assert.Equal(t, h.sum, 8.0)
	assert.Equal(t, h.min, 3.0)
	assert.Equal(t, h.max, 5.0)

	h.add(1.0, baseValue)
	assert.ArrayEqual(t, h.buckets, []int64{1, 0, 1, 1})
	assert.Equal(t, h.count, int64(3))
	assert.Equal(t, h.sum, 9.0)
	assert.Equal(t, h.min, 1.0)
	assert.Equal(t, h.max, 5.0)

	h.add(4.0, baseValue)
	assert.ArrayEqual(t, h.buckets, []int64{1, 0, 2, 1})
	assert.Equal(t, h.count, int64(4))
	assert.Equal(t, h.sum, 13.0)
	assert.Equal(t, h.min, 1.0)
	assert.Equal(t, h.max, 5.0)

	h.add(10.0, baseValue)
	assert.ArrayEqual(t, h.buckets, []int64{1, 0, 2, 1})
	assert.Equal(t, h.count, int64(5))
	assert.Equal(t, h.sum, 23.0)
	assert.Equal(t, h.min, 1.0)
	assert.Equal(t, h.max, 10.0)
}
