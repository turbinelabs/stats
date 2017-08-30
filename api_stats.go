package stats

import (
	"fmt"
	"log"
	"os"
	"time"

	apiflags "github.com/turbinelabs/api/client/flags"
	"github.com/turbinelabs/api/service/stats"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
)

const (
	// DefaultClientApp is the clientApp used when constructing an api stats
	// client. If a specific clientApp is required, use SetStatsClientFromFlags.
	DefaultClientApp = "github.com/turbinelabs/stats"

	unspecified = "unspecified"
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

// SetZoneKeyFromFlags specifies a pre-configured
// apiflags.ZoneKeyFromFlags to use when creating an API stats sender.
func SetZoneKeyFromFlags(zoneKeyFromFlags apiflags.ZoneKeyFromFlags) APIStatsOption {
	return func(ff *apiStatsFromFlags) {
		ff.zoneKeyFromFlags = zoneKeyFromFlags
	}
}

// SetLogger specifies a custom Logger to use when constructing an
// stats.StatsService.
func SetLogger(logger *log.Logger) APIStatsOption {
	return func(ff *apiStatsFromFlags) {
		ff.logger = logger
	}
}

func newAPIStatsFromFlags(fs tbnflag.FlagSet, options ...APIStatsOption) statsFromFlags {
	ff := &apiStatsFromFlags{
		flagScope:               fs.GetScope(),
		latchingSenderFromFlags: newLatchingSenderFromFlags(fs, true),
	}

	for _, apply := range options {
		apply(ff)
	}

	if ff.zoneKeyFromFlags == nil {
		ff.zoneKeyFromFlags = apiflags.NewZoneKeyFromFlags(fs)
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

	return ff
}

type apiStatsFromFlags struct {
	flagScope               string
	logger                  *log.Logger
	zoneKeyFromFlags        apiflags.ZoneKeyFromFlags
	statsClientFromFlags    apiflags.StatsClientFromFlags
	latchingSenderFromFlags *latchingSenderFromFlags
}

func (ff *apiStatsFromFlags) Validate() error {
	if ff.statsClientFromFlags.APIKey() == "" {
		return fmt.Errorf("--%skey must be specified", ff.flagScope)
	}

	if ff.zoneKeyFromFlags.ZoneName() == "" {
		return fmt.Errorf("--%szone-name must be specified", ff.flagScope)
	}

	if err := ff.statsClientFromFlags.Validate(); err != nil {
		return err
	}

	return ff.latchingSenderFromFlags.Validate()
}

func (ff *apiStatsFromFlags) Make() (Stats, error) {
	logger := ff.logger
	if logger == nil {
		logger = log.New(os.Stderr, "stats: ", log.LstdFlags)
	}

	statsClient, err := ff.statsClientFromFlags.MakeV2(logger)
	if err != nil {
		return nil, err
	}

	sender := &apiSender{
		svc:    statsClient,
		source: unspecified,
		zone:   ff.zoneKeyFromFlags.ZoneName(),
	}

	wrappedSender := ff.latchingSenderFromFlags.Make(sender, apiCleaner)

	underlying := newFromSender(wrappedSender, apiCleaner, "", false)

	return &apiStats{underlying, sender}, nil
}

type apiStats struct {
	Stats

	apiSender *apiSender
}

// Scope always returns this Stats implementation as API stats do not
// support scoping.
func (a *apiStats) Scope(s string, ss ...string) Stats {
	return a
}

// AddTags filters out tags named source, node, and zone. The source
// and zone tags alter the source and zone used when making API stats
// forwarding calls. The node tag is ignored. All other tags are
// treated normally.
func (s *apiStats) AddTags(tags ...Tag) {
	for _, tag := range tags {
		switch tag.K {
		case NodeTag:
			// ignore

		case SourceTag:
			s.apiSender.source = tag.V

		case ZoneTag:
			s.apiSender.zone = tag.V

		default:
			s.Stats.AddTags(tag)
		}
	}
}

// NewAPIStats creates a Stats that uses the given stats.StatsServiceV2
// to forward arbitrary stats with an unspecified source and zone. The
// source and zone may be subsequently overridden by invoking AddTags
// with tags named SourceTag and ZoneTag.
func NewAPIStats(svc stats.StatsServiceV2) Stats {
	sender := &apiSender{
		svc:    svc,
		source: unspecified,
		zone:   unspecified,
	}
	underlying := newFromSender(sender, apiCleaner, "", false)

	return &apiStats{underlying, sender}
}

// NewLatchingAPIStats creates a Stats as in NewAPIStats, but with latching enabled.
func NewLatchingAPIStats(
	svc stats.StatsServiceV2,
	window time.Duration,
	baseValue float64,
	numBuckets int,
) Stats {
	sender := &apiSender{
		svc:    svc,
		source: unspecified,
		zone:   unspecified,
	}

	wrappedSender := newLatchingSender(
		sender,
		apiCleaner,
		latchWindow(window),
		latchBuckets(baseValue, numBuckets),
	)

	underlying := newFromSender(wrappedSender, apiCleaner, "", false)

	return &apiStats{underlying, sender}
}
