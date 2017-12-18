package stats

import (
	"strings"
	"unicode"

	"github.com/rs/xstats/prometheus"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
)

// CleanPrometheusTagName strips characters which prometheus considers
// illegal from tag names. Tag names must match the regular
// expression: "[a-zA-Z_][a-zA-Z0-9_]*". Leading digits cause an
// underscore to be prepended to the name. All other illegal
// characters are converted to underscores.
func CleanPrometheusTagName(s string) string {
	if s == "" {
		return s
	}

	first := s[0]
	if '0' <= first && first <= '9' {
		s = "_" + s
	}

	return strings.Map(
		func(r rune) rune {
			if r > unicode.MaxASCII {
				return '_'
			}

			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return r
			}

			return '_'
		},
		s,
	)
}

// CleanPrometheusStatName strips characters which prometheus
// considers illegal from stat names. Stat names must match the
// regular expression: "[a-zA-Z_:][a-zA-Z0-9_:]*". Leading digits
// cause an underscore to be prepended to the name. All other illegal
// characters are converted to underscores.
func CleanPrometheusStatName(s string) string {
	if s == "" {
		return s
	}

	first := s[0]
	if '0' <= first && first <= '9' {
		s = "_" + s
	}

	return strings.Map(
		func(r rune) rune {
			if r > unicode.MaxASCII {
				return '_'
			}

			if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == ':' {
				return r
			}

			return '_'
		},
		s,
	)
}

var prometheusCleaner = cleaner{
	cleanStatName: CleanPrometheusStatName,
	cleanTagName:  mkSequence(filterTimestamp, CleanPrometheusTagName),
	cleanTagValue: identity,
	scopeDelim:    ":",
	tagDelim:      ":",
}

type prometheusFromFlags struct {
	flagScope string
	addr      tbnflag.HostPort
	scope     string
}

func newPrometheusFromFlags(fs tbnflag.FlagSet) statsFromFlags {
	ff := &prometheusFromFlags{flagScope: fs.GetScope()}

	fs.HostPortVar(
		&ff.addr,
		"addr",
		tbnflag.NewHostPort("0.0.0.0:9102"),
		"Specifies the listener address for Prometheus scraping.",
	)

	fs.StringVar(
		&ff.scope,
		"scope",
		"",
		"If specified, prepends the given scope to metric names.",
	)

	return ff
}

func (ff *prometheusFromFlags) Validate() error {
	return nil
}

func (ff *prometheusFromFlags) Make() (Stats, error) {
	return newFromSender(prometheus.New(ff.addr.Addr()), prometheusCleaner, ff.scope, true), nil
}
