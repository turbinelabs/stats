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
	"testing"
	"time"

	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
	"github.com/turbinelabs/test/assert"
)

func TestDogstatsdBackend(t *testing.T) {
	l := mkListener(t)
	defer l.Close()

	addr := l.Addr(t)
	_, port, err := tbnstrings.SplitHostPort(addr)
	assert.Nil(t, err)

	dogstatsdFromFlags := &dogstatsdFromFlags{
		&statsdFromFlags{
			host:          "127.0.0.1",
			port:          port,
			flushInterval: 10 * time.Millisecond,
			dsff:          &demuxingSenderFromFlags{},
			lsff:          &latchingSenderFromFlags{},
		},
	}

	stats, err := dogstatsdFromFlags.Make()
	assert.Nil(t, err)
	defer stats.Close()

	scope := stats.Scope("prefix")

	scope.Count("count", 2.0, NewKVTag("taggity", "tag"))
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("prefix.count:%f|c|#taggity:tag\n", 2.0))

	scope.Gauge("gauge", 3.0)
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("prefix.gauge:%f|g\n", 3.0))
}

func TestDogstatsdBackendWithScope(t *testing.T) {
	l := mkListener(t)
	defer l.Close()

	addr := l.Addr(t)
	_, port, err := tbnstrings.SplitHostPort(addr)
	assert.Nil(t, err)

	dogstatsdFromFlags := &dogstatsdFromFlags{
		&statsdFromFlags{
			host:          "127.0.0.1",
			port:          port,
			flushInterval: 10 * time.Millisecond,
			dsff:          &demuxingSenderFromFlags{},
			lsff:          &latchingSenderFromFlags{},
			scope:         "x",
		},
	}

	stats, err := dogstatsdFromFlags.Make()
	assert.Nil(t, err)
	defer stats.Close()

	scope := stats.Scope("prefix")

	scope.Count("count", 2.0, NewKVTag("taggity", "tag"))
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("x.prefix.count:%f|c|#taggity:tag\n", 2.0))

	scope.Gauge("gauge", 3.0)
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("x.prefix.gauge:%f|g\n", 3.0))
}

func TestDogstatsdBackendWithTagDemuxing(t *testing.T) {
	l := mkListener(t)
	defer l.Close()

	addr := l.Addr(t)
	_, port, err := tbnstrings.SplitHostPort(addr)
	assert.Nil(t, err)

	dogstatsdFromFlags := &dogstatsdFromFlags{
		&statsdFromFlags{
			host:          "127.0.0.1",
			port:          port,
			flushInterval: 10 * time.Millisecond,
			dsff:          &demuxingSenderFromFlags{config: "taggity=/(.).*/,t"},
			lsff:          &latchingSenderFromFlags{},
			scope:         "x",
		},
	}

	stats, err := dogstatsdFromFlags.Make()
	assert.Nil(t, err)
	defer stats.Close()

	scope := stats.Scope("prefix")

	scope.Count("count", 2.0, NewKVTag("taggity", "tag"))
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("x.prefix.count:%f|c|#taggity:tag,t:t\n", 2.0))

	scope.Gauge("gauge", 3.0)
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("x.prefix.gauge:%f|g\n", 3.0))
}

func TestDogstatsdCleanerCleanStatName(t *testing.T) {
	testCases := [][]string{
		{"ok", "ok"},
		{"no:colons", "nocolons"},
	}

	for _, tc := range testCases {
		assert.Equal(t, dogstatsdCleaner.cleanStatName(tc[0]), tc[1])
	}
}

func TestDogstatsdCleanerTagToString(t *testing.T) {
	testCases := []struct {
		tag      Tag
		expected string
	}{
		{
			tag:      NewKVTag("x", "y"),
			expected: `x:y`,
		},
		{
			tag:      NewKVTag("a:b", "x:y"),
			expected: "a_b:x_y",
		},
		{
			tag:      NewKVTag("a|b", "x|y"),
			expected: "a_b:x_y",
		},
		{
			tag:      NewKVTag("a,b", "x,y"),
			expected: "a_b:x_y",
		},
		{
			tag:      NewKVTag("x y", "x: \U0001F600"),
			expected: "x y:x_ \U0001F600",
		},
		{
			tag:      NewKVTag(TimestampTag, "1234567890"),
			expected: "",
		},
	}

	for i, tc := range testCases {
		assert.Group(
			fmt.Sprintf(
				"Tag(%s, %s) (testcase %d of %d)",
				tc.tag.K,
				tc.tag.V,
				i+1,
				len(testCases),
			),
			t,
			func(g *assert.G) {
				got := dogstatsdCleaner.tagToString(tc.tag)
				assert.Equal(g, got, tc.expected)
			},
		)
	}
}
