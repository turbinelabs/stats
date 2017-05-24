package stats

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rs/xstats"
	"github.com/rs/xstats/statsd"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
)

type wavefrontSender struct {
	s xstats.Sender
}

// Gauge implements xstats.Sender interface
func (wfs *wavefrontSender) Gauge(stat string, value float64, tags ...string) {
	wfs.s.Gauge(mkStatName(stat, tags), value)
}

// Count implements xstats.Sender interface
func (wfs *wavefrontSender) Count(stat string, count float64, tags ...string) {
	wfs.s.Count(mkStatName(stat, tags), count)
}

// Histogram implements xstats.Sender interface
func (wfs *wavefrontSender) Histogram(stat string, value float64, tags ...string) {
	wfs.s.Histogram(mkStatName(stat, tags), value)
}

// Timing implements xstats.Sender interface
func (wfs *wavefrontSender) Timing(stat string, duration time.Duration, tags ...string) {
	wfs.s.Timing(mkStatName(stat, tags), duration)
}

// Close implements io.Closer interface
func (wfs *wavefrontSender) Close() error {
	if c, ok := wfs.s.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// mkStatName generates a new stat name encoding the given tags
func mkStatName(stat string, tags []string) string {
	// TODO: there's definitely some missing escaping here
	res := make([]string, len(tags)+1)
	res[0] = stat
	for i, tag := range tags {
		res[i+1] = fmt.Sprintf("~%s", tag)
	}

	return strings.Join(res, "")
}

type wavefrontFromFlags struct {
	*statsdFromFlags
}

func newWavefrontFromFlags(fs tbnflag.FlagSet) statsFromFlags {
	return &wavefrontFromFlags{newStatsdFromFlags(fs, wavefrontName)}
}

func (ff *wavefrontFromFlags) Make() (Stats, error) {
	w, err := ff.mkUDPWriter()
	if err != nil {
		return nil, err
	}
	return newFromSender(&wavefrontSender{statsd.New(w, ff.flushInterval)}, statsdCleaner), nil
}
