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

var (
	cleanWavefront = func(s string) string {
		return strings.Map(
			func(r rune) rune {
				switch {
				case r >= '0' && r <= '9':
					return r
				case r >= 'A' && r <= 'Z':
					return r
				case r >= 'a' && r <= 'z':
					return r
				case r == '_' || r == '-' || r == '.':
					return r
				default:
					return -1
				}
			},
			s,
		)
	}

	cleanWavefrontTagValue = func(s string) string {
		// "~" is used to separate tags in wavefront's statsd
		// plugin and must be stripped.
		s = strings.Replace(s, "~", "", -1)

		return fmt.Sprintf(`"%s"`, strings.Replace(s, `"`, `\"`, -1))
	}
)

// Per https://community.wavefront.com/docs/DOC-1031.
// Stat names: ascii alphanumeric, hyphen, underscore, period. Forward
//             slash and comma require quoting.
// Tag names: ascii alphanumeric, hyphen, underscore, period.
// Tag values: quoted strings allow any value, including quotes (by
//             backslash escaping)
// The wavefront statsd plugin passes tags through without
// modification or further escaping.
var wavefrontCleaner = cleaner{
	cleanStatName: cleanWavefront,
	cleanTagName:  cleanWavefront,
	cleanTagValue: cleanWavefrontTagValue,
	tagDelim:      "=",
	scopeDelim:    ".",
}

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
	return newFromSender(
		&wavefrontSender{statsd.NewMaxPacket(w, ff.flushInterval, ff.maxPacketLen)},
		wavefrontCleaner,
	), nil
}
