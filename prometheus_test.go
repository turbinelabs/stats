package stats

import (
	"testing"

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
	}

	for _, tc := range testCases {
		got := prometheusCleaner.tagToString(tc.tag)
		assert.Equal(t, got, tc.expected)
	}
}
