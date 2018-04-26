package stats

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/turbinelabs/idgen"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/nonstdlib/ptr"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
)

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

const (
	apiStatsName   = "api"
	dogstatsdName  = "dogstatsd"
	prometheusName = "prometheus"
	statsdName     = "statsd"
	wavefrontName  = "wavefront"

	maxNodeLen   = 256
	maxSourceLen = 256
)

// FromFlags produces a Stats from command line flags.
type FromFlags interface {
	// Validate validates the command line flags.
	Validate() error

	// Make constructs a Stats from command line flags.
	Make() (Stats, error)

	// Node returns the value of the node tag (must be called after Make).
	Node() string

	// Source returns the value of the source tag (must be called after Make).
	Source() string
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

// DefaultBackends configures NewFromFlags with default backends (that
// may be overridden by command line flags). Unknown backends are
// ignored.
func DefaultBackends(backends ...string) Option {
	return func(ff *fromFlagsOptions) {
		ff.defaultBackends = backends
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

	var defaultBackends []string
	if len(ffOpts.defaultBackends) > 0 {
		for _, backend := range ffOpts.defaultBackends {
			backend = strings.ToLower(backend)
			if _, ok := sffMap[backend]; ok {
				defaultBackends = append(defaultBackends, backend)
			}
		}
	}

	sort.Strings(backends)

	ff := &fromFlags{
		statsFromFlagses: sffMap,
		flagScope:        fs.GetScope(),
		backends:         tbnflag.NewStringsWithConstraint(backends...),
		tags:             tbnflag.NewStrings(),
	}
	ff.backends.ResetDefault(defaultBackends...)

	ff.initFlags(fs)
	return ff
}

type fromFlags struct {
	statsFromFlagses map[string]statsFromFlags
	flagScope        string
	backends         tbnflag.Strings
	nodeTag          string
	sourceTag        string
	uniqueSourceTag  string
	tags             tbnflag.Strings

	resolved          bool
	resolvedNodeTag   string
	resolvedSourceTag string
	resolvedTags      []Tag
}

type fromFlagsOptions struct {
	enableAPIStats  bool
	apiStatsOptions []APIStatsOption
	defaultBackends []string
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
		`If set, specifies the source to use when submitting stats to backends. Equivalent to adding "--{{PREFIX}}tags=source=value" to the command line. In either case, a UUID is appended to the value to insure that it is unique across proxies. Cannot be combined with --{{PREFIX}}unique-source.`,
	)

	fs.StringVar(
		&ff.uniqueSourceTag,
		"unique-source",
		"",
		`If set, specifies the source to use when submitting stats to backends. Equivalent to adding "--{{PREFIX}}tags=source=value" to the command line. Unlike --{{PREFIX}}source, failing to specify a unique value may prevent stats from being recorded correctly. Cannot be combined with --{{PREFIX}}source.`,
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

	sourceTag, nodeTag, _, err := ff.parseTags()
	if err != nil {
		return err
	}

	if ff.nodeTag != "" {
		if nodeTag != nil {
			return fmt.Errorf("cannot combine --%stags=node=... with --%[1]snode", ff.flagScope)
		}

		nodeTag = &ff.nodeTag
	}

	if ff.sourceTag != "" || ff.uniqueSourceTag != "" {
		if sourceTag != nil || (ff.sourceTag != "" && ff.uniqueSourceTag != "") {
			return fmt.Errorf(
				"cannot combine --%stags=source=... with --%[1]ssource or --%[1]sunique-source",
				ff.flagScope,
			)
		}

		sourceTag = &ff.sourceTag
	}

	if len(ptr.StringValue(nodeTag)) > maxNodeLen {
		return fmt.Errorf(
			"--%snode or --%[1]stags=node=... may not be longer than %d bytes",
			ff.flagScope,
			maxNodeLen,
		)
	}

	if len(ptr.StringValue(sourceTag)) > maxSourceLen {
		return fmt.Errorf(
			"--%ssource or --%[1]stags=source=... may not be longer than %d bytes",
			ff.flagScope,
			maxSourceLen,
		)
	}

	if len(ff.uniqueSourceTag) > maxSourceLen {
		return fmt.Errorf(
			"--%sunique-source may not be longer than %d bytes",
			ff.flagScope,
			maxSourceLen,
		)
	}

	return nil
}

func (ff *fromFlags) parseTags() (*string, *string, []Tag, error) {
	var source, node *string

	result := make([]Tag, 0, len(ff.tags.Strings))
	for _, tag := range ff.tags.Strings {
		key, value := tbnstrings.SplitFirstEqual(tag)
		switch key {
		case SourceTag:
			if value != "" {
				if source != nil {
					return nil, nil, nil, errors.New("cannot specify multiple tags named source")
				}
				source = &value
			}
		case NodeTag:
			if value != "" {
				if node != nil {
					return nil, nil, nil, errors.New("cannot specify multiple tags named node")
				}
				node = &value
			}
		default:
			result = append(result, NewKVTag(key, value))
		}
	}

	return source, node, result, nil
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

	stats := NewMulti(statses...)

	if !ff.resolved {
		sourceTag, nodeTag, tags, err := ff.parseTags()
		if err != nil {
			return nil, err
		}

		if sourceTag != nil {
			ff.sourceTag = *sourceTag
		}

		ff.resolvedNodeTag = ff.nodeTag
		if nodeTag != nil {
			ff.resolvedNodeTag = *nodeTag
		}

		if ff.uniqueSourceTag != "" {
			ff.resolvedSourceTag = ff.uniqueSourceTag
		} else {
			uuid, err := idgen.NewUUID()()
			if err != nil {
				return nil, err
			}

			if ff.sourceTag != "" {
				ff.resolvedSourceTag = fmt.Sprintf("%s-%s", ff.sourceTag, uuid)
			} else {
				ff.resolvedSourceTag = string(uuid)
			}
		}

		ff.resolvedTags = tags
		ff.resolved = true
	}

	stats.AddTags(ff.resolvedTags...)

	if ff.resolvedNodeTag != "" {
		stats.AddTags(NewKVTag(NodeTag, ff.resolvedNodeTag))
	}

	if ff.resolvedSourceTag != "" {
		stats.AddTags(NewKVTag(SourceTag, ff.resolvedSourceTag))
	}

	return stats, nil
}

func (ff *fromFlags) Node() string {
	return ff.resolvedNodeTag
}

func (ff *fromFlags) Source() string {
	return ff.resolvedSourceTag
}
