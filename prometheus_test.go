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

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/test/assert"
)

func TestCleanPrometheusTagName(t *testing.T) {
	testCases := [][2]string{
		{"ok", "ok"},
		{"OK123", "OK123"},
		{"_123Ok", "_123Ok"},
		{"123", "_123"},
		{"abc.123", "abc_123"},
		{"-xyz-", "_xyz_"},
		{"", ""},
		{"nøpe", "n_pe"},
	}

	for _, tc := range testCases {
		assert.Equal(t, CleanPrometheusTagName(tc[0]), tc[1])
	}
}

func TestCleanPrometheusStatName(t *testing.T) {
	testCases := [][2]string{
		{"ok", "ok"},
		{"OK123", "OK123"},
		{"_123Ok", "_123Ok"},
		{"123", "_123"},
		{"abc.123", "abc_123"},
		{"abc/123", "abc_123"},
		{"abc:123", "abc:123"},
		{"a/b:c/d", "a_b:c_d"},
		{"-xyz-", "_xyz_"},
		{":xyz", ":xyz"},
		{"", ""},
		{"nøpe", "n_pe"},
	}

	for _, tc := range testCases {
		assert.Equal(t, CleanPrometheusStatName(tc[0]), tc[1])
	}
}

func TestPrometheusCleanerTagToString(t *testing.T) {
	testCases := []struct {
		tag      Tag
		expected string
	}{
		{
			tag:      NewKVTag("x", "y"),
			expected: `x:y`,
		},
		{
			tag:      NewKVTag("x y", "x: \U0001F600"),
			expected: "x_y:x: \U0001F600",
		},
		{
			tag:      NewKVTag(TimestampTag, "1234567890"),
			expected: "",
		},
	}

	for _, tc := range testCases {
		got := prometheusCleaner.tagToString(tc.tag)
		assert.Equal(t, got, tc.expected)
	}
}

func TestPrometheusMake(t *testing.T) {
	flags := &prometheusFromFlags{
		flagScope: "prometheus",
		addr:      tbnflag.NewHostPort(":0"),
		scope:     "",
	}

	s, err := flags.Make()
	assert.NonNil(t, s)
	assert.Nil(t, err)
}
