package handler

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

import (
	"flag"
	"log"

	apihttp "github.com/turbinelabs/api/http"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/server/handler"
)

// AuthorizerFromFlags constructs a handler.Authorizer from command
// line flags.
type AuthorizerFromFlags interface {
	// Constructs a handler.Authorizer from command line flags
	// with the given log.Logger.
	Make(*log.Logger) (handler.Authorizer, error)
}

// NewAPIAuthorizerFromFlags constructs a new AuthorizerFromFlags for
// a handler.Authorizer that uses the Turbine API to authorize
// requests.
func NewAPIAuthorizerFromFlags(flagset *flag.FlagSet) AuthorizerFromFlags {
	prefixedFlagSet := tbnflag.NewPrefixedFlagSet(
		flagset,
		"api",
		"API",
	)

	ff := &apiAuthFromFlags{}

	ff.clientFromFlags = apihttp.NewFromFlags("api.turbinelabs.io", prefixedFlagSet)

	return ff
}

type apiAuthFromFlags struct {
	clientFromFlags apihttp.FromFlags
}

func (ff *apiAuthFromFlags) Make(log *log.Logger) (handler.Authorizer, error) {
	client := ff.clientFromFlags.MakeClient()

	endpoint, err := ff.clientFromFlags.MakeEndpoint()
	if err != nil {
		return nil, err
	}

	auth := apiAuthorizer{
		client:   client,
		endpoint: endpoint,
		log:      log,
	}

	return auth.wrap, nil
}
