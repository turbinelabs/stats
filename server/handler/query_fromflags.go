package handler

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

import (
	"errors"
	"log"

	"github.com/turbinelabs/nonstdlib/executor"
	"github.com/turbinelabs/nonstdlib/flag"
)

// QueryHandlerFromFlags constructs a QueryHandler from command line flags.
type QueryHandlerFromFlags interface {
	Validate(useMockData bool) error

	// Constructs a new QueryHandler with the given log.Logger.
	Make(log *log.Logger, verboseLogging bool, useMockData bool) (QueryHandler, error)
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

	ff.executorFromFlags = executor.NewFromFlags(flagset.Scope("exec", "executor"))

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
	verboseLogging bool,
	useMockData bool,
) (QueryHandler, error) {
	if useMockData {
		return NewMockQueryHandler(), nil
	} else {
		return NewQueryHandler(
			ff.wavefrontServerUrl,
			ff.wavefrontApiToken,
			verboseLogging,
			ff.executorFromFlags.Make(log),
		)
	}
}
