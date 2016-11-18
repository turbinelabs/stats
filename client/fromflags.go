package client

import (
	"errors"
	"log"
	"time"

	"github.com/turbinelabs/api/client/flags"
	"github.com/turbinelabs/nonstdlib/executor"
	"github.com/turbinelabs/nonstdlib/flag"
)

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

const (
	DefaultMaxBatchDelay = 1 * time.Second
	DefaultMaxBatchSize  = 100
)

// FromFlags validates and constructs a client.StatsClient from command line
// flags.
type FromFlags interface {
	Validate() error

	// Constructs a client.StatsClient using the given Executor and Logger.
	Make(executor.Executor, *log.Logger) (StatsClient, error)

	// Returns the API Key used to construct the client.StatsClient.
	APIKey() string
}

type ClientOption func(*fromFlags)

func WithAPIConfigFromFlags(apiConfigFromFlags flags.APIConfigFromFlags) ClientOption {
	return func(ff *fromFlags) {
		ff.apiConfigFromFlags = apiConfigFromFlags
	}
}

func NewFromFlags(pfs *flag.PrefixedFlagSet, options ...ClientOption) FromFlags {
	ff := &fromFlags{}

	for _, option := range options {
		option(ff)
	}

	if ff.apiConfigFromFlags == nil {
		ff.apiConfigFromFlags = flags.NewPrefixedAPIConfigFromFlags(pfs)
	}

	pfs.BoolVar(
		&ff.useBatching,
		"batch",
		true,
		"If true, {{NAME}} requests are batched together for performance.",
	)

	pfs.DurationVar(
		&ff.maxBatchDelay,
		"max-batch-delay",
		DefaultMaxBatchDelay,
		"If batching is enabled, the maximum amount of time requests are held before transmission",
	)

	pfs.IntVar(
		&ff.maxBatchSize,
		"max-batch-size",
		DefaultMaxBatchSize,
		"If batching is enabled, the maximum number of requests that will be combined.",
	)

	return ff

}

type fromFlags struct {
	apiConfigFromFlags flags.APIConfigFromFlags
	useBatching        bool
	maxBatchDelay      time.Duration
	maxBatchSize       int

	cachedClient StatsClient
}

func (ff *fromFlags) Validate() error {
	if ff.useBatching {
		if ff.maxBatchDelay < 1*time.Second {
			return errors.New(
				"max-batch-delay may not be less than 1 second",
			)
		}

		if ff.maxBatchSize < 1 {
			return errors.New(
				"max-batch-size may not be less than 1",
			)
		}
	}

	return nil
}

func (ff *fromFlags) Make(exec executor.Executor, logger *log.Logger) (StatsClient, error) {
	if ff.cachedClient != nil {
		return ff.cachedClient, nil
	}

	client := ff.apiConfigFromFlags.MakeClient()

	endpoint, err := ff.apiConfigFromFlags.MakeEndpoint()
	if err != nil {
		return nil, err
	}

	var stats StatsClient
	if ff.useBatching {
		stats, err = NewBatchingStatsClient(
			ff.maxBatchDelay,
			ff.maxBatchSize,
			endpoint,
			ff.apiConfigFromFlags.APIKey(),
			client,
			exec,
			logger,
		)
	} else {
		stats, err = NewStatsClient(endpoint, ff.apiConfigFromFlags.APIKey(), client, exec)
	}

	if err != nil {
		return nil, err
	}

	ff.cachedClient = stats

	return stats, nil
}

func (ff *fromFlags) APIKey() string {
	return ff.apiConfigFromFlags.APIKey()
}
