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
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/turbinelabs/test/assert"
)

func TestNewDemuxingSender(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	underlying := newMockXstatsSender(ctrl)

	testDemuxTag, err := newDemuxTag("a", "^foo(.*)$", []string{"b"})
	assert.Nil(t, err)

	s := newDemuxingSender(underlying, testCleaner, []demuxTag{testDemuxTag})
	sImpl, ok := s.(*demuxingSender)
	assert.True(t, ok)
	assert.SameInstance(t, sImpl.underlying, underlying)
	assert.Equal(t, len(sImpl.tags), 1)
	assert.NonNil(t, sImpl.tags["a"])
	assert.NotSameInstance(t, sImpl.tags["a"], &testDemuxTag)

	s = newDemuxingSender(underlying, testCleaner, []demuxTag{})
	assert.SameInstance(t, s, underlying)
}

func TestNewDemuxTag(t *testing.T) {
	dt, err := newDemuxTag("name", "*bad pattern*", nil)
	assert.DeepEqual(t, dt, demuxTag{})
	assert.ErrorContains(t, err, "error parsing regexp")

	dt, err = newDemuxTag("name", "x+", nil)
	assert.DeepEqual(t, dt, demuxTag{})
	assert.ErrorContains(t, err, `pattern "x+" contains no subexpressions`)

	dt, err = newDemuxTag("name", "(x+)", nil)
	assert.DeepEqual(t, dt, demuxTag{})
	assert.ErrorContains(t, err, `contains 1 subexpressions, but 0 names were provided`)

	dt, err = newDemuxTag("name", "(x+)", []string{"a", "b"})
	assert.DeepEqual(t, dt, demuxTag{})
	assert.ErrorContains(t, err, `contains 1 subexpressions, but 2 names were provided`)

	dt, err = newDemuxTag("name", "(x+) (y+)", []string{"a", "b"})
	assert.DeepEqual(t, dt, demuxTag{
		name:            "name",
		regex:           regexp.MustCompile("(x+) (y+)"),
		mappedNames:     []string{"a", "b"},
		replaceOriginal: false,
	})
	assert.Nil(t, err)

	dt, err = newDemuxTag("name", "(x+|z*) (y+)", []string{"a", "name"})
	assert.DeepEqual(t, dt, demuxTag{
		name:            "name",
		regex:           regexp.MustCompile("(x+|z*) (y+)"),
		mappedNames:     []string{"a", "name"},
		replaceOriginal: true,
	})
	assert.Nil(t, err)
}

func TestDemuxTagDemux(t *testing.T) {
	dt, err := newDemuxTag("name", "^foo=(.+),bar=(.*)$", []string{"foo", "bar"})
	assert.Nil(t, err)

	tags, ok := dt.demux("name:foo=123,bar=456", "foo=123,bar=456", ":")
	assert.ArrayEqual(t, tags, []string{"name:foo=123,bar=456", "foo:123", "bar:456"})
	assert.True(t, ok)

	tags, ok = dt.demux("name:foo=123,bar=", "foo=123,bar=", ":")
	assert.ArrayEqual(t, tags, []string{"name:foo=123,bar=", "foo:123", "bar:"})
	assert.True(t, ok)

	tags, ok = dt.demux("name->foo=123,bar=456", "foo=123,bar=456", "->")
	assert.ArrayEqual(t, tags, []string{"name->foo=123,bar=456", "foo->123", "bar->456"})
	assert.True(t, ok)

	tags, ok = dt.demux("name:foo,bar", "foo=123,bar", ":")
	assert.Nil(t, tags)
	assert.False(t, ok)

	// Nested subexpressions
	dt, err = newDemuxTag(
		"node",
		"^((.+)-(prod|canary|dev)-[a-z0-9]+)-(.+)$",
		[]string{"node", "app", "stage", "version"},
	)
	assert.Nil(t, err)

	tags, ok = dt.demux(
		"node:the-app-canary-pod871-2018-05-09-1552a",
		"the-app-canary-pod871-2018-05-09-1552a",
		":",
	)
	assert.ArrayEqual(
		t,
		tags,
		[]string{
			"node:the-app-canary-pod871",
			"app:the-app",
			"stage:canary",
			"version:2018-05-09-1552a",
		},
	)
}

func TestDemuxTagDemuxWithReplaceOriginal(t *testing.T) {
	dt, err := newDemuxTag("x", "^(.+),y=(.*)$", []string{"x", "why"})
	assert.Nil(t, err)

	tags, ok := dt.demux("x:123,y=456", "123,y=456", ":")
	assert.ArrayEqual(t, tags, []string{"x:123", "why:456"})
	assert.True(t, ok)

	tags, ok = dt.demux("x:123,y", "123,y", ":")
	assert.Nil(t, tags)
	assert.False(t, ok)
}

func TestDemuxingSenderDemuxTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	const T1 = "a=b"
	const T2 = "c=d"
	const T3 = "e=f"

	dt1, err := newDemuxTag("d1", "x:(.*):y:(.*)", []string{"d1x", "d1y"})
	assert.Nil(t, err)

	dt2, err := newDemuxTag("d2x", "(.*):y:(.*)", []string{"d2x", "d2y"})
	assert.Nil(t, err)

	underlying := newMockXstatsSender(ctrl)

	s := newDemuxingSender(underlying, testCleaner, []demuxTag{dt1, dt2})
	sImpl, ok := s.(*demuxingSender)
	assert.True(t, ok)

	noMatches := [][]string{
		{T1, "c=d"},
		{"d1=nomatch"},
		{"d2x=nomatch"},
	}

	for _, tags := range noMatches {
		assert.ArrayEqual(t, sImpl.demuxTags(tags), tags)
	}

	matches := []struct {
		name         string
		inputTags    []string
		expectedTags []string
	}{
		{
			name:         "just replacements",
			inputTags:    []string{"d1=x:1:y:2", "d2x=3:y:4"},
			expectedTags: []string{"d1=x:1:y:2", "d1x=1", "d1y=2", "d2x=3", "d2y=4"},
		},
		{
			name:         "replacements and non-matches",
			inputTags:    []string{T1, "d1=x:1:y:2", T2, "d2x=3:y:4", T3},
			expectedTags: []string{T1, "d1=x:1:y:2", "d1x=1", "d1y=2", T2, "d2x=3", "d2y=4", T3},
		},
		{
			name:         "replacements and non-matches 2",
			inputTags:    []string{"d1=x:1:y:2", T1, "d2x=3:y:4"},
			expectedTags: []string{"d1=x:1:y:2", "d1x=1", "d1y=2", T1, "d2x=3", "d2y=4"},
		},
		{
			name:         "just replacements, reverse config order",
			inputTags:    []string{"d2x=3:y:4", "d1=x:1:y:2"},
			expectedTags: []string{"d2x=3", "d2y=4", "d1=x:1:y:2", "d1x=1", "d1y=2"},
		},
		{
			name:         "replacements and non-matches, reverse config order",
			inputTags:    []string{"d2x=3:y:4", T1, "d1=x:1:y:2"},
			expectedTags: []string{"d2x=3", "d2y=4", T1, "d1=x:1:y:2", "d1x=1", "d1y=2"},
		},
		{
			name:         "replacements and duplicate tags",
			inputTags:    []string{"d1=x:1:y:2", "d2x=3:y:4", "d1x=!", "d2y=!!"},
			expectedTags: []string{"d1=x:1:y:2", "d1x=1", "d1y=2", "d2x=3", "d2y=4", "d1x=!", "d2y=!!"},
		},
		{
			name:         "replacements and duplicate tags, reverse order",
			inputTags:    []string{"d1x=!", "d2y=!!", "d1=x:1:y:2", "d2x=3:y:4"},
			expectedTags: []string{"d1x=!", "d2y=!!", "d1=x:1:y:2", "d1x=1", "d1y=2", "d2x=3", "d2y=4"},
		},
	}

	for i, match := range matches {
		assert.Group(
			fmt.Sprintf("match[%d]: %s", i, match.name),
			t,
			func(g *assert.G) {
				assert.ArrayEqual(g, sImpl.demuxTags(match.inputTags), match.expectedTags)
			},
		)
	}
}

func TestDemuxSenderAPIMethods(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	dt, err := newDemuxTag("t", "([A-Z]+)", []string{"alphat"})
	assert.Nil(t, err)

	underlying := newMockXstatsSender(ctrl)

	s := newDemuxingSender(underlying, testCleaner, []demuxTag{dt})

	tags := []string{"t=ABC"}
	expectedTags := []interface{}{"t=ABC", "alphat=ABC"}

	underlying.EXPECT().Count("w", 1.0, expectedTags...)
	underlying.EXPECT().Gauge("x", 2.0, expectedTags...)
	underlying.EXPECT().Histogram("y", 3.0, expectedTags...)
	underlying.EXPECT().Timing("z", 4*time.Second, expectedTags...)

	s.Count("w", 1.0, tags...)
	s.Gauge("x", 2.0, tags...)
	s.Histogram("y", 3.0, tags...)
	s.Timing("z", 4*time.Second, tags...)
}
