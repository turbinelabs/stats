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
	"errors"
	"log"
	"os"
	"time"

	apiflags "github.com/turbinelabs/api/client/flags"
	"github.com/turbinelabs/api/service/stats"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/nonstdlib/log/console"
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

// SetZoneFromFlags specifies a pre-configured
// apiflags.ZoneFromFlags to use when creating an API stats sender.
func SetZoneFromFlags(zoneFromFlags apiflags.ZoneFromFlags) APIStatsOption {
	return func(ff *apiStatsFromFlags) {
		ff.zoneFromFlags = zoneFromFlags
	}
}

// SetLogger specifies a custom Logger to use when constructing an
// stats.StatsService.
func SetLogger(logger *log.Logger) APIStatsOption {
	return func(ff *apiStatsFromFlags) {
		ff.logger = logger
	}
}

// AllowEmptyAPIKey configures the API StatsFromFlags to produce a NopStats in
// the case where no API key is specified
func AllowEmptyAPIKey() APIStatsOption {
	return func(ff *apiStatsFromFlags) {
		ff.allowEmptyAPIKey = true
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
	zoneFromFlags           apiflags.ZoneFromFlags
	statsClientFromFlags    apiflags.StatsClientFromFlags
	latchingSenderFromFlags *latchingSenderFromFlags
	allowEmptyAPIKey        bool
}

func (ff *apiStatsFromFlags) Validate() error {
	if ff.statsClientFromFlags.APIKey() == "" {
		if ff.allowEmptyAPIKey {
			return nil
		}
		return errors.New("API key must be specified for API stats backend")
	}

	// ok for zone to be empty, but if a zoneFromFlags was configured, it had
	// better have a value.
	if ff.zoneFromFlags != nil && ff.zoneFromFlags.Name() == "" {
		return errors.New("zone-name must be specified for API stats backend")
	}

	if err := ff.statsClientFromFlags.Validate(); err != nil {
		return err
	}

	return ff.latchingSenderFromFlags.Validate()
}

func (ff *apiStatsFromFlags) Make() (Stats, error) {
	if ff.allowEmptyAPIKey && ff.statsClientFromFlags.APIKey() == "" {
		console.Info().Println("No API key specified, the API stats backend will not be configured.")
		return NewNoopStats(), nil
	}
	logger := ff.logger
	if logger == nil {
		logger = log.New(os.Stderr, "stats: ", log.LstdFlags)
	}

	statsClient, err := ff.statsClientFromFlags.Make(logger)
	if err != nil {
		return nil, err
	}

	var zone string
	if ff.zoneFromFlags != nil {
		zone = ff.zoneFromFlags.Name()
	}

	sender := &apiSender{
		svc:    statsClient,
		source: unspecified,
		zone:   zone,
	}

	wrappedSender := ff.latchingSenderFromFlags.Make(sender, apiCleaner)

	underlying := newFromSender(wrappedSender, apiCleaner, "", nil, false)

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

// AddTags filters out tags named source, proxy, node, and zone. The source, proxy,
// and zone tags alter the source, proxy, node, and zone used when making API stats
// forwarding calls.
func (a *apiStats) AddTags(tags ...Tag) {
	for _, tag := range tags {
		switch tag.K {
		case NodeTag:
			a.apiSender.node = tag.V

		case ProxyTag:
			a.apiSender.proxy = tag.V

		case SourceTag:
			a.apiSender.source = tag.V

		case ZoneTag:
			a.apiSender.zone = tag.V

		default:
			a.Stats.AddTags(tag)
		}
	}
}

// NewAPIStats creates a Stats that uses the given stats.StatsService
// to forward arbitrary stats with an unspecified source and zone. The
// source and zone may be subsequently overridden by invoking AddTags
// with tags named SourceTag and ZoneTag.
func NewAPIStats(svc stats.StatsService) Stats {
	sender := &apiSender{
		svc:    svc,
		source: unspecified,
		zone:   unspecified,
	}
	underlying := newFromSender(sender, apiCleaner, "", nil, false)

	return &apiStats{underlying, sender}
}

// NewLatchingAPIStats creates a Stats as in NewAPIStats, but with latching enabled.
func NewLatchingAPIStats(
	svc stats.StatsService,
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

	underlying := newFromSender(wrappedSender, apiCleaner, "", nil, false)

	return &apiStats{underlying, sender}
}
