package stats

import (
	"errors"
	"fmt"
	"sort"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
)

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

const (
	apiStatsName   = "api"
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

// Option is an opaquely-typed option for NewFromFlags.
type Option func(*fromFlagsOptions)

// EnableAPIStatsBackend enables the API stats backend.
func EnableAPIStatsBackend() Option {
	return func(ff *fromFlagsOptions) {
		ff.enableAPIStats = true
	}
}

// APIStatsOptions configures NewFromFlags to pass APIStatsOption
// values to the API Stats backend.
func APIStatsOptions(opts ...APIStatsOption) Option {
	return func(ff *fromFlagsOptions) {
		ff.apiStatsOptions = append(ff.apiStatsOptions, opts...)
	}
}

// NewFromFlags produces a FromFlags configured by the given flagset
// and options.
func NewFromFlags(fs tbnflag.FlagSet, options ...Option) FromFlags {
	ffOpts := &fromFlagsOptions{}
	for _, apply := range options {
		apply(ffOpts)
	}

	backends := []string{
		dogstatsdName,
		prometheusName,
		wavefrontName,
		statsdName,
	}

	sffMap := map[string]statsFromFlags{
		dogstatsdName:  newDogstatsdFromFlags(fs.Scope(dogstatsdName, "")),
		prometheusName: newPrometheusFromFlags(fs.Scope(prometheusName, "")),
		wavefrontName:  newWavefrontFromFlags(fs.Scope(wavefrontName, "")),
		statsdName:     newStatsdFromFlags(fs.Scope(statsdName, "")),
	}

	if ffOpts.enableAPIStats {
		backends = append(backends, apiStatsName)

		sffMap[apiStatsName] = newAPIStatsFromFlags(
			fs.Scope(apiStatsName, ""),
			ffOpts.apiStatsOptions...,
		)
	}

	sort.Strings(backends)

	ff := &fromFlags{
		backends:         tbnflag.NewStringsWithConstraint(backends...),
		tags:             tbnflag.NewStrings(),
		statsFromFlagses: sffMap,
	}

	ff.initFlags(fs)
	return ff
}

type fromFlags struct {
	statsFromFlagses    map[string]statsFromFlags
	backends            tbnflag.Strings
	nodeTag             string
	sourceTag           string
	tags                tbnflag.Strings
	classifyStatusCodes bool
}

type fromFlagsOptions struct {
	enableAPIStats  bool
	apiStatsOptions []APIStatsOption
}

func (ff *fromFlags) initFlags(fs tbnflag.FlagSet) {
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
		fmt.Sprintf(
			`If enabled, stats with tagged with "%s" will automatically gain another tag, "%s", with a value of "%s", "%s", "%s" or "%s". If the "%[1]s" value is not numeric, the "%[2]s" tag is omitted.`,
			StatusCodeTag,
			StatusClassTag,
			StatusClassSuccess,
			StatusClassRedirect,
			StatusClassClientErr,
			StatusClassServerErr,
		),
	)
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

	seenNode := false
	seenSource := false
	for _, tag := range ff.parseTags() {
		if tag.K == "node" {
			if ff.nodeTag != "" {
				return errors.New("cannot combine --tags=node=... and --node")
			}

			if seenNode {
				return errors.New("cannot specify multiple tags named node")
			}

			seenNode = true
		}

		if tag.K == "source" {
			if ff.sourceTag != "" {
				return errors.New("cannot combine --tags=source=... and --source")
			}

			if seenSource {
				return errors.New("cannot specify multiple tags named source")
			}

			seenSource = true
		}
	}

	return nil
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
	stats.AddTags(ff.parseTags()...)

	if ff.nodeTag != "" {
		stats.AddTags(NewKVTag(NodeTag, ff.nodeTag))
	}

	if ff.sourceTag != "" {
		stats.AddTags(NewKVTag(SourceTag, ff.sourceTag))
	}

	return stats, nil
}
