package server

import (
	"flag"
	"log"
	"os"

	"github.com/turbinelabs/cli/flags"
	"github.com/turbinelabs/server"
	serverhandler "github.com/turbinelabs/server/handler"
	"github.com/turbinelabs/server/header"
	"github.com/turbinelabs/stats/server/handler"
	"github.com/turbinelabs/stats/server/route"
	"github.com/turbinelabs/statsd"
)

// FromFlags validates and constructs a stats Server from command line
// flags.
type FromFlags interface {
	// Validates the stats server's flags.
	Validate() error

	// Constructs an un-started Server from command line flags.
	Make() (server.Server, error)
}

// NewFromFlags produces a new FromFlags for the given FlagSet,
// initializing its flags as appropriate.
func NewFromFlags(flagset *flag.FlagSet) FromFlags {
	ff := &fromFlags{}

	flagset.BoolVar(&ff.devMode, "dev", false, "Developer mode: API keys are not checked")

	serverFlagSet := flags.NewPrefixedFlagSet(flagset, "listener", "stats listener")

	ff.ServerFromFlags = server.NewFromFlags(serverFlagSet)
	ff.StatsFromFlags = statsd.NewFromFlags(flagset)
	ff.AuthorizerFromFlags = handler.NewAPIAuthorizerFromFlags(flagset)
	ff.MetricsCollectorFromFlags = handler.NewMetricsCollectorFromFlags(flagset)

	return ff
}

type fromFlags struct {
	devMode                   bool
	ServerFromFlags           server.FromFlags
	StatsFromFlags            statsd.FromFlags
	AuthorizerFromFlags       handler.AuthorizerFromFlags
	MetricsCollectorFromFlags handler.MetricsCollectorFromFlags
}

func (cfg *fromFlags) Validate() error {
	if err := cfg.ServerFromFlags.Validate(); err != nil {
		return err
	}

	if err := cfg.MetricsCollectorFromFlags.Validate(); err != nil {
		return err
	}

	return nil
}

func (ff *fromFlags) Make() (server.Server, error) {
	logger := log.New(os.Stderr, "", log.LstdFlags)

	stats, err := ff.StatsFromFlags.Make()
	if err != nil {
		return nil, err
	}

	var authorizer serverhandler.Authorizer
	if ff.devMode {
		authorizer = serverhandler.SimpleHeaderAuth(header.APIKey)
	} else {
		authorizer, err = ff.AuthorizerFromFlags.Make(logger)
		if err != nil {
			return nil, err
		}
	}

	collector, err := ff.MetricsCollectorFromFlags.Make(logger)
	if err != nil {
		return nil, err
	}

	routes := route.MkRoutes(stats, authorizer, collector)

	server, err := ff.ServerFromFlags.Make(logger, logger, stats, routes)
	if err != nil {
		return nil, err
	}

	server.DeferClose(collector)

	if ff.devMode {
		logger.Println("stats-server: dev-mode")
	} else {
		logger.Println("stats-server")
	}

	return server, nil
}
