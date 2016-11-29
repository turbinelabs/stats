package handler

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

import (
	"errors"
	"log"
	"time"

	"github.com/turbinelabs/nonstdlib/executor"
	"github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/nonstdlib/stats"
)

// QueryHandlerFromFlags constructs a QueryHandler from command line flags.
type QueryHandlerFromFlags interface {
	Validate(useMockData bool) error

	// Constructs a new QueryHandler with the given log.Logger and
	// stats.Stats.
	Make(
		log *log.Logger,
		stats stats.Stats,
		verboseLogging bool,
		useMockData bool,
	) (QueryHandler, error)
}

func NewQueryHandlerFromFlags(flagset *flag.PrefixedFlagSet) QueryHandlerFromFlags {
	ff := &queryHandlerFromFlags{}

	flagset.StringVar(
		&ff.wavefrontServerUrl,
		"url",
		DefaultWavefrontServerUrl,
		"Sets the wavefront server URL.",
	)

	flagset.StringVar(
		&ff.wavefrontApiToken,
		"token",
		"",
		"Authentication token for {{NAME}}. Required unless developer mode is used to generate mock data.",
	)

	ff.executorFromFlags =
		executor.NewFromFlagsWithDefaults(
			flagset.Scope("exec", "executor"),
			executor.FromFlagsDefaults{
				AttemptTimeout: 5 * time.Second,
				Timeout:        30 * time.Second,
			},
		)

	return ff
}

type queryHandlerFromFlags struct {
	wavefrontServerUrl string
	wavefrontApiToken  string
	executorFromFlags  executor.FromFlags
}

func (ff *queryHandlerFromFlags) Validate(useMockData bool) error {
	if !useMockData && ff.wavefrontApiToken == "" {
		return errors.New("--wavefront-api.token is a required flag")
	}

	return nil
}

func (ff *queryHandlerFromFlags) Make(
	log *log.Logger,
	stats stats.Stats,
	verboseLogging bool,
	useMockData bool,
) (QueryHandler, error) {
	if useMockData {
		return NewMockQueryHandler(), nil
	} else {
		exec := ff.executorFromFlags.Make(log)
		exec.SetStats(stats)

		return NewQueryHandler(
			ff.wavefrontServerUrl,
			ff.wavefrontApiToken,
			verboseLogging,
			exec,
		)
	}
}
