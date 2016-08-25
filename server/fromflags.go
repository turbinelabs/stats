package server

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/turbinelabs/arrays/indexof"
	"github.com/turbinelabs/cli/flags"
	"github.com/turbinelabs/server"
	serverhandler "github.com/turbinelabs/server/handler"
	"github.com/turbinelabs/server/header"
	"github.com/turbinelabs/stats/server/handler"
	"github.com/turbinelabs/stats/server/route"
	"github.com/turbinelabs/statsd"
)

const (
	noAuthMode = "noauth"
	mockMode   = "mock"
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
	ff := &fromFlags{
		devMode: flags.NewStringsWithConstraint([]string{noAuthMode, mockMode}),
	}

	flagset.Var(
		&ff.devMode,
		"dev",
		"Developer `modes`. Accepts a comma-separated list of modes. "+
			"Possible modes are "+noAuthMode+" and "+mockMode+". "+
			"The "+noAuthMode+" mode disables API key checking. "+
			"The "+mockMode+" mode returns mock data only.",
	)

	serverFlagSet := flags.NewPrefixedFlagSet(flagset, "listener", "stats listener")
	ff.ServerFromFlags = server.NewFromFlags(serverFlagSet)

	ff.StatsFromFlags = statsd.NewFromFlags(serverFlagSet.Scope("stats", "internal server"))
	ff.AuthorizerFromFlags = handler.NewAPIAuthorizerFromFlags(flagset)
	ff.MetricsCollectorFromFlags = handler.NewMetricsCollectorFromFlags(flagset)

	return ff
}

type fromFlags struct {
	devMode                   flags.Strings
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

	noAuth := indexof.String(ff.devMode.Strings, noAuthMode) != indexof.NotFound
	mockData := indexof.String(ff.devMode.Strings, mockMode) != indexof.NotFound

	var authorizer serverhandler.Authorizer
	if noAuth {
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

	var queryHandler handler.QueryHandler
	if mockData {
		queryHandler = handler.NewMockQueryHandler()
	} else {
		queryHandler = handler.NewQueryHandler()
	}

	routes := route.MkRoutes(stats, authorizer, collector, queryHandler)

	server, err := ff.ServerFromFlags.Make(logger, logger, stats, routes)
	if err != nil {
		return nil, err
	}

	server.DeferClose(collector)

	if noAuth || mockData {
		devModes := []string{}
		if noAuth {
			devModes = append(devModes, "no-auth")
		}
		if mockData {
			devModes = append(devModes, "mock-data")
		}
		logger.Printf("stats-server: dev-mode: %s", strings.Join(devModes, " "))
	} else {
		logger.Println("stats-server")
	}

	return server, nil
}
