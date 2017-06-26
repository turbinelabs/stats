package stats

import (
	"errors"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
)

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

const (
	dogstatsdName  = "dogstatsd"
	prometheusName = "prometheus"
	statsdName     = "statsd"
	wavefrontName  = "wavefront"
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

	fs.StringVar(
		&ff.sourceTag,
		"source",
		"",
		`If set, specifies the source to use when submitting stats to backends. Equivalent to adding "--{{PREFIX}}tags=source=value" to the command line.`,
	)

	fs.StringVar(
		&ff.nodeTag,
		"node",
		"",
		`If set, specifies the node to use when submitting stats to backends. Equivalent to adding "--{{PREFIX}}tags=node=value" to the command line.`,
	)

	fs.Var(
		&ff.tags,
		"tags",
		`Tags to be included with every stat. May be comma-delimited or specified more than once. Should be of the form "<key>=<value>" or "tag"`,
	)

	fs.BoolVar(
		&ff.classifyStatusCodes,
		"classify-status-codes",
		true,
		`If enabled, stats with a tagged with "status_code" will automatically gain another tag, "status_class", with a value of "success", "redirect", "client_error" or "server_error". If the "status_code" value is not numeric, the "status_class" tag is omitted.`,
	)

	return ff
}

type statsFromFlags interface {
	Validate() error
	Make(classifyStatusCodes bool) (Stats, error)
}

type fromFlags struct {
	statsFromFlagses    map[string]statsFromFlags
	backends            tbnflag.Strings
	nodeTag             string
	sourceTag           string
	tags                tbnflag.Strings
	classifyStatusCodes bool
}

func (ff *fromFlags) parseTags() []Tag {
	result := make([]Tag, 0, len(ff.tags.Strings))
	for _, tag := range ff.tags.Strings {
		key, value := tbnstrings.SplitFirstEqual(tag)
		if value == "" {
			result = append(result, NewTag(key))
		} else {
			result = append(result, NewKVTag(key, value))
		}
	}
	return result
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

	for _, tag := range ff.parseTags() {
		if tag.K == "node" && ff.nodeTag != "" {
			return errors.New("cannot combine --tags=node=... and --node")
		}

		if tag.K == "source" && ff.sourceTag != "" {
			return errors.New("cannot combine --tags=source=... and --source")
		}
	}

	return nil
}

func (ff *fromFlags) Make() (Stats, error) {
	statses := make([]Stats, 0, len(ff.statsFromFlagses))
	for _, backend := range ff.backends.Strings {
		if sff, ok := ff.statsFromFlagses[backend]; ok {
			sender, err := sff.Make(ff.classifyStatusCodes)
			if err != nil {
				return nil, err
			}

			statses = append(statses, sender)
		}
	}

	stats := NewMulti(statses...)
	for _, tag := range ff.tags.Strings {
		key, value := tbnstrings.SplitFirstEqual(tag)
		if value == "" {
			stats.AddTags(NewTag(key))
		} else {
			stats.AddTags(NewKVTag(key, value))
		}
	}

	if ff.nodeTag != "" {
		stats.AddTags(NewKVTag("node", ff.nodeTag))
	}

	if ff.sourceTag != "" {
		stats.AddTags(NewKVTag("source", ff.sourceTag))
	}

	return stats, nil
}
