package stats

import (
	"fmt"
	"log"
	"os"

	apiflags "github.com/turbinelabs/api/client/flags"
	"github.com/turbinelabs/api/service/stats"
	"github.com/turbinelabs/nonstdlib/executor"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
)

const (
	// DefaultClientApp is the clientApp used when constructing an api stats
	// client. If a specific clientApp is required, use SetStatsClientFromFlags.
	DefaultClientApp = "github.com/turbinelabs/stats"
)

// APIStatsOption is a configuration option the the API stats backend.
type APIStatsOption func(*apiStatsFromFlags)

// SetStatsClientFromFlags specifies a pre-configured
// apiflags.StatsClientFromFlags to use when creating a
// stats.StatsService.
func SetStatsClientFromFlags(statsClientFromFlags apiflags.StatsClientFromFlags) APIStatsOption {
	return func(ff *apiStatsFromFlags) {
		ff.statsClientFromFlags = statsClientFromFlags
	}
}

// SetExecutorFromFlags specifies a pre-configured executor.FromFlags
// to use when creating a stats.StatsService.
func SetExecutorFromFlags(execFromFlags executor.FromFlags) APIStatsOption {
	return func(ff *apiStatsFromFlags) {
		ff.execFromFlags = execFromFlags
	}
}

// SetLogger specifies a custom Logger to use when constructing an
// executor.Executor and stats.StatsService.
func SetLogger(logger *log.Logger) APIStatsOption {
	return func(ff *apiStatsFromFlags) {
		ff.logger = logger
	}
}

func newAPIStatsFromFlags(fs tbnflag.FlagSet, options ...APIStatsOption) statsFromFlags {
	ff := &apiStatsFromFlags{
		flagScope:               fs.GetScope(),
		latchingSenderFromFlags: newLatchingSenderFromFlags(fs),
	}

	for _, apply := range options {
		apply(ff)
	}

	if ff.statsClientFromFlags == nil {
		apiConfigFromFlags := apiflags.NewAPIConfigFromFlags(
			fs,
			apiflags.APIConfigSetAPIAuthKeyFromFlags(
				apiflags.NewAPIAuthKeyFromFlags(
					fs,
					apiflags.APIAuthKeyFlagsOptional(),
				),
			),
		)

		ff.statsClientFromFlags = apiflags.NewStatsClientFromFlags(
			DefaultClientApp,
			fs,
			apiflags.StatsClientWithAPIConfigFromFlags(apiConfigFromFlags),
		)
	}

	if ff.execFromFlags == nil {
		ff.execFromFlags = executor.NewFromFlags(fs)
	}

	return ff
}

type apiStatsFromFlags struct {
	flagScope               string
	logger                  *log.Logger
	statsClientFromFlags    apiflags.StatsClientFromFlags
	execFromFlags           executor.FromFlags
	latchingSenderFromFlags *latchingSenderFromFlags
}

func (ff *apiStatsFromFlags) Validate() error {
	if ff.statsClientFromFlags.APIKey() == "" {
		return fmt.Errorf("--%skey must be specified", ff.flagScope)
	}

	if err := ff.statsClientFromFlags.Validate(); err != nil {
		return err
	}

	return ff.latchingSenderFromFlags.Validate()
}

func (ff *apiStatsFromFlags) Make(classifyStatusCodes bool) (Stats, error) {
	logger := ff.logger
	if logger == nil {
		logger = log.New(os.Stderr, "stats: ", log.LstdFlags)
	}

	exec := ff.execFromFlags.Make(logger)

	statsClient, err := ff.statsClientFromFlags.Make(exec, logger)
	if err != nil {
		return nil, err
	}

	sender := &apiSender{
		svc:    statsClient,
		source: "unspecified",
	}

	wrappedSender := ff.latchingSenderFromFlags.Make(sender, apiCleaner)

	underlying := newFromSender(wrappedSender, apiCleaner, false)

	return &apiStats{underlying, sender}, nil
}

type apiStats struct {
	Stats

	apiSender *apiSender
}

// AddTags filters out tags named source and alters the source used
// when making API stats forwarding calls. All other tags are treated
// normally.
func (s *apiStats) AddTags(tags ...Tag) {
	for _, tag := range tags {
		if tag.K == SourceTag {
			s.apiSender.source = tag.V
		} else {
			s.Stats.AddTags(tag)
		}
	}
}

// NewAPIStats creates a Stats that uses the given stats.StatsService
// to forward arbitrary stats with an unspecified source. The source
// may be subsequently overridden by invoking AddTags with a tag named
// "source".
func NewAPIStats(svc stats.StatsService) Stats {
	sender := &apiSender{
		svc:    svc,
		source: "unspecified",
	}
	underlying := newFromSender(sender, apiCleaner, false)

	return &apiStats{underlying, sender}
}
