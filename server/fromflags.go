package server

import (
	"errors"
	"flag"
	"log"
	"os"
	"strings"

	"github.com/turbinelabs/arrays/indexof"
	"github.com/turbinelabs/cli/flags"
	"github.com/turbinelabs/server"
	"github.com/turbinelabs/server/cors"
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
		devMode: flags.NewStringsWithConstraint(noAuthMode, mockMode),
	}

	flagset.Var(
		&ff.devMode,
		"dev",
		"Developer `modes`. Accepts a comma-separated list of modes. "+
			"Possible modes are "+noAuthMode+" and "+mockMode+". "+
			"The "+noAuthMode+" mode disables API key checking. "+
			"The "+mockMode+" mode returns mock data only.",
	)

	flagset.StringVar(
		&ff.wavefrontApiToken,
		"wavefront-api.token",
		"",
		"Authentication token for the wavefront API. Required unless developer mode is used to generate mock data.",
	)

	serverFlagSet := flags.NewPrefixedFlagSet(flagset, "listener", "stats listener")
	ff.ServerFromFlags = server.NewFromFlags(serverFlagSet)

	ff.StatsFromFlags = statsd.NewFromFlags(serverFlagSet.Scope("stats", "internal server"))
	ff.AuthorizerFromFlags = handler.NewAPIAuthorizerFromFlags(flagset)
	ff.MetricsCollectorFromFlags = handler.NewMetricsCollectorFromFlags(flagset)
	ff.CORSFromFlags = cors.NewFromFlags(flags.NewPrefixedFlagSet(flagset, "cors", "stats API server"))

	return ff
}

type fromFlags struct {
	devMode                   flags.Strings
	wavefrontApiToken         string
	ServerFromFlags           server.FromFlags
	StatsFromFlags            statsd.FromFlags
	CORSFromFlags             cors.FromFlags
	AuthorizerFromFlags       handler.AuthorizerFromFlags
	MetricsCollectorFromFlags handler.MetricsCollectorFromFlags
}

func (ff *fromFlags) devModeNoAuth() bool {
	return indexof.String(ff.devMode.Strings, noAuthMode) != indexof.NotFound
}

func (ff *fromFlags) devModeMockData() bool {
	return indexof.String(ff.devMode.Strings, mockMode) != indexof.NotFound
}

func (ff *fromFlags) Validate() error {
	if err := ff.ServerFromFlags.Validate(); err != nil {
		return err
	}

	if err := ff.MetricsCollectorFromFlags.Validate(); err != nil {
		return err
	}

	if !ff.devModeMockData() && ff.wavefrontApiToken == "" {
		return errors.New("--wavefront-api.token is a required flag")
	}

	return nil
}

func (ff *fromFlags) Make() (server.Server, error) {
	logger := log.New(os.Stderr, "", log.LstdFlags)

	stats, err := ff.StatsFromFlags.Make()
	if err != nil {
		return nil, err
	}

	noAuth := ff.devModeNoAuth()
	mockData := ff.devModeMockData()

	var authorizer serverhandler.Authorizer
	if noAuth {
		authorizer = serverhandler.SimpleHeaderAuth(header.APIKey).AndThen(
			handler.MockAuthorizer,
		)
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
		queryHandler = handler.NewQueryHandler(ff.wavefrontApiToken)
	}

	allowedOrigins := ff.CORSFromFlags.AllowedOrigins()

	routes := route.MkRoutes(stats, authorizer, collector, queryHandler, allowedOrigins)

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
