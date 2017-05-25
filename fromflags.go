package stats

import (
	"errors"
	"time"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
)

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

const (
	defaultFlushInterval = 500 * time.Millisecond
	dogstatsdName        = "dogstatsd"
	prometheusName       = "prometheus"
	statsdName           = "statsd"
	wavefrontName        = "wavefront"
)

// FromFlags produces a Stats from configuration flags
type FromFlags interface {
	Validate() error
	Make() (Stats, error)
}

// NewFromFlags produces a FromFlags configured by the given flagset
func NewFromFlags(fs tbnflag.FlagSet) FromFlags {
	ff := &fromFlags{
		backends: tbnflag.NewStringsWithConstraint(
			dogstatsdName,
			wavefrontName,
			prometheusName,
			statsdName,
		),
		tags: tbnflag.NewStrings(),
		statsFromFlagses: map[string]statsFromFlags{
			dogstatsdName:  newDogstatsdFromFlags(fs),
			wavefrontName:  newWavefrontFromFlags(fs),
			prometheusName: newPrometheusFromFlags(fs),
			statsdName:     newStatsdFromFlags(fs, "statsd"),
		},
	}

	fs.Var(
		&ff.backends,
		"backends",
		"Selects which stats backend(s) to use.",
	)

	fs.Var(
		&ff.tags,
		"tags",
		`Tags to be included with every stat. May be comma-delimited or specified more than once. Should be of the form "<key>=<value>" or "tag"`,
	)

	return ff
}

type statsFromFlags interface {
	Validate() error
	Make() (Stats, error)
}

type fromFlags struct {
	statsFromFlagses map[string]statsFromFlags
	backends         tbnflag.Strings
	tags             tbnflag.Strings
}

func (ff *fromFlags) Validate() error {
	if len(ff.backends.Strings) == 0 {
		return errors.New("no backends specified")
	}

	for _, backend := range ff.backends.Strings {
		if sff, ok := ff.statsFromFlagses[backend]; ok {
			if err := sff.Validate(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (ff *fromFlags) Make() (Stats, error) {
	statses := make([]Stats, 0, len(ff.statsFromFlagses))
	for _, backend := range ff.backends.Strings {
		if sff, ok := ff.statsFromFlagses[backend]; ok {
			sender, err := sff.Make()
			if err != nil {
				return nil, err
			}
			statses = append(statses, sender)
		}
	}

	stats := NewMulti(statses)
	for _, tag := range ff.tags.Strings {
		key, value := tbnstrings.SplitFirstEqual(tag)
		if value == "" {
			stats.AddTags(NewTag(key))
		} else {
			stats.AddTags(NewKVTag(key, value))
		}
	}

	return stats, nil
}
