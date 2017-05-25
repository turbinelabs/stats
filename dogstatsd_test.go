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
