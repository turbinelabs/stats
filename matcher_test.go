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
	reflect "reflect"
	"testing"
	"time"

	"github.com/rs/xstats/statsd"
	"github.com/turbinelabs/test/assert"
	testio "github.com/turbinelabs/test/io"
)

func TestMatcher(t *testing.T) {
	x := &xStats{}
	m := Matcher(x)
	assert.DeepEqual(t, reflect.TypeOf(m), reflect.TypeOf(&xstatsEqual{}))
	xseImpl := m.(*xstatsEqual)
	assert.SameInstance(t, xseImpl.expected, x)

	a := &apiStats{}
	m = Matcher(a)
	assert.DeepEqual(t, reflect.TypeOf(m), reflect.TypeOf(&apiStatsEqual{}))
	apiImpl := m.(*apiStatsEqual)
	assert.SameInstance(t, apiImpl.expected, a)

	assert.Panic(t, func() {
		Matcher(NewNoopStats())
	})
}

func TestXStatsEqual(t *testing.T) {
	sender1 := statsd.New(testio.NewNoopWriter(), time.Second)
	sender2 := statsd.New(testio.NewNoopWriter(), time.Minute)

	cleaner1 := cleaner{
		cleanStatName: identity,
		cleanTagName:  identity,
		cleanTagValue: identity,
		tagDelim:      ":",
		scopeDelim:    "/",
	}

	cleaner2 := cleaner{
		cleanStatName: identity,
		cleanTagName:  identity,
		cleanTagValue: identity,
		tagDelim:      ":",
		scopeDelim:    "/",
	}

	x1a := newFromSender(sender1, testCleaner, "s", nil, true).(*xStats)
	x1b := newFromSender(sender1, testCleaner, "s", nil, true).(*xStats)
	x2 := newFromSender(sender2, testCleaner, "s", nil, true).(*xStats)
	x3a := newFromSender(sender1, cleaner1, "s", nil, true).(*xStats)
	x3b := newFromSender(sender1, cleaner2, "s", nil, true).(*xStats)

	assert.True(t, xstatsEqual{x1a}.Matches(x1b))
	assert.False(t, xstatsEqual{x1a}.Matches(x2))
	assert.False(t, xstatsEqual{x1a}.Matches(x3a))
	assert.True(t, xstatsEqual{x3a}.Matches(x3b))

	assert.False(t, xstatsEqual{x1a}.Matches(NewNoopStats()))

	assert.MatchesRegex(t, xstatsEqual{x1a}.String(), `xstatsEqual\(.+\)`)
}

func TestAPIStatsEqual(t *testing.T) {
	sender1 := &apiSender{source: "s1", zone: "z"}
	sender2 := &apiSender{source: "s2", zone: "z"}

	cleaner1 := cleaner{
		cleanStatName: identity,
		cleanTagName:  identity,
		cleanTagValue: identity,
		tagDelim:      ":",
		scopeDelim:    "/",
	}

	cleaner2 := cleaner{
		cleanStatName: identity,
		cleanTagName:  identity,
		cleanTagValue: identity,
		tagDelim:      ":",
		scopeDelim:    "/",
	}

	x1a := &apiStats{newFromSender(sender1, testCleaner, "s", nil, true), sender1}
	x1b := &apiStats{newFromSender(sender1, testCleaner, "s", nil, true).(*xStats), sender1}
	x2 := &apiStats{newFromSender(sender2, testCleaner, "s", nil, true).(*xStats), sender2}
	x3a := &apiStats{newFromSender(sender1, cleaner1, "s", nil, true).(*xStats), sender1}
	x3b := &apiStats{newFromSender(sender1, cleaner2, "s", nil, true).(*xStats), sender1}

	assert.True(t, apiStatsEqual{x1a}.Matches(x1b))
	assert.False(t, apiStatsEqual{x1a}.Matches(x2))
	assert.False(t, apiStatsEqual{x1a}.Matches(x3a))
	assert.True(t, apiStatsEqual{x3a}.Matches(x3b))

	assert.False(t, apiStatsEqual{x1a}.Matches(NewNoopStats()))

	assert.MatchesRegex(t, apiStatsEqual{x1a}.String(), `apiStatsEqual\(.+\)`)
}

func TestTagMatches(t *testing.T) {
	assert.Panic(t, func() { TagMatches("x", "(") })

	m := TagMatches("k", "a.+c")

	assert.True(t, m.Matches(NewKVTag("k", "abc")))
	assert.False(t, m.Matches(NewKVTag("not-k", "abc")))
	assert.False(t, m.Matches(NewKVTag("k", "xyz")))
	assert.False(t, m.Matches("not-a-tag"))

	assert.MatchesRegex(t, m.String(), `tagMatches\(k=~/.+/\)`)
}
