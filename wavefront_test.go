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

	"github.com/golang/mock/gomock"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
	"github.com/turbinelabs/test/assert"
)

func TestWavefrontSenderGauge(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()
	mockSender := newMockXstatsSender(ctrl)
	wfs := &wavefrontSender{mockSender}
	mockSender.EXPECT().Gauge("foo~bar=baz~blar=blaz", float64(1234))
	wfs.Gauge("foo", 1234, "bar=baz", "blar=blaz")
}

func TestWavefrontSenderCount(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()
	mockSender := newMockXstatsSender(ctrl)
	wfs := &wavefrontSender{mockSender}
	mockSender.EXPECT().Count("foo~bar=baz~blar=blaz", float64(1234))
	wfs.Count("foo", 1234, "bar=baz", "blar=blaz")
}

func TestWavefrontSenderHistogram(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()
	mockSender := newMockXstatsSender(ctrl)
	wfs := &wavefrontSender{mockSender}
	mockSender.EXPECT().Histogram("foo~bar=baz~blar=blaz", float64(1234))
	wfs.Histogram("foo", 1234, "bar=baz", "blar=blaz")
}

func TestWavefrontSenderTiming(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()
	mockSender := newMockXstatsSender(ctrl)
	wfs := &wavefrontSender{mockSender}
	mockSender.EXPECT().Timing("foo~bar=baz~blar=blaz", 1234*time.Millisecond)
	wfs.Timing("foo", 1234*time.Millisecond, "bar=baz", "blar=blaz")
}

func TestWavefrontBackend(t *testing.T) {
	l := mkListener(t)
	defer l.Close()

	addr := l.Addr(t)
	_, port, err := tbnstrings.SplitHostPort(addr)
	assert.Nil(t, err)

	wavefrontFromFlags := &wavefrontFromFlags{
		&statsdFromFlags{
			host:          "127.0.0.1",
			port:          port,
			flushInterval: 10 * time.Millisecond,
			lsff:          &latchingSenderFromFlags{},
		},
	}

	stats, err := wavefrontFromFlags.Make()
	assert.Nil(t, err)
	defer stats.Close()

	scope := stats.Scope("prefix")

	scope.Count("count", 2.0, NewKVTag("taggity", "tag"))
	assert.Equal(t, <-l.Msgs, fmt.Sprintf(`prefix.count~taggity="tag":%f|c`+"\n", 2.0))

	scope.Gauge("gauge", 3.0)
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("prefix.gauge:%f|g\n", 3.0))
}

func TestWavefrontBackendWithScope(t *testing.T) {
	l := mkListener(t)
	defer l.Close()

	addr := l.Addr(t)
	_, port, err := tbnstrings.SplitHostPort(addr)
	assert.Nil(t, err)

	wavefrontFromFlags := &wavefrontFromFlags{
		&statsdFromFlags{
			host:          "127.0.0.1",
			port:          port,
			flushInterval: 10 * time.Millisecond,
			lsff:          &latchingSenderFromFlags{},
			scope:         "x",
		},
	}

	stats, err := wavefrontFromFlags.Make()
	assert.Nil(t, err)
	defer stats.Close()

	scope := stats.Scope("prefix")

	scope.Count("count", 2.0, NewKVTag("taggity", "tag"))
	assert.Equal(t, <-l.Msgs, fmt.Sprintf(`x.prefix.count~taggity="tag":%f|c`+"\n", 2.0))

	scope.Gauge("gauge", 3.0)
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("x.prefix.gauge:%f|g\n", 3.0))
}

func TestWavefrontCleanerToTagString(t *testing.T) {
	testCases := []struct {
		tag      Tag
		expected string
	}{
		{
			tag:      NewKVTag("x", "y"),
			expected: `x="y"`,
		},
		{
			tag:      NewKVTag("has Space", "y"),
			expected: `hasSpace="y"`,
		},
		{
			tag:      NewKVTag("x!@#$%^&*x0", "y"),
			expected: `xx0="y"`,
		},
		{
			tag:      NewKVTag("x-x_x.x", "y"),
			expected: `x-x_x.x="y"`,
		},
		{
			tag:      NewKVTag("x\U0001f600x", "y"),
			expected: `xx="y"`,
		},
		{
			tag:      NewKVTag("x", "y z"),
			expected: `x="y z"`,
		},
		{
			tag:      NewKVTag("x", `"quoted"`),
			expected: `x="\"quoted\""`,
		},
		{
			tag:      NewKVTag(TimestampTag, "1234567890"),
			expected: "",
		},
	}

	for _, tc := range testCases {
		got := wavefrontCleaner.tagToString(tc.tag)
		assert.Equal(t, got, tc.expected)
	}
}
