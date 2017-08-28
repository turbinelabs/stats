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
