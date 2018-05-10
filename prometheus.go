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
	return newFromSender(
		prometheus.New(ff.addr.Addr()), prometheusCleaner, ff.scope, nil, true,
	), nil
}
