package handler

import (
	"log"
	"net/http"

	"github.com/turbinelabs/api"
	apiclient "github.com/turbinelabs/api/client"
	apihttp "github.com/turbinelabs/api/http"
	httperr "github.com/turbinelabs/api/http/error"
	"github.com/turbinelabs/api/service"
	tbnauth "github.com/turbinelabs/server/auth"
	"github.com/turbinelabs/server/handler"
	"github.com/turbinelabs/stats/server/handler/requestcontext"
)

const NoAuthOrgKey = "test-org-key"

type apiAuthorizer struct {
	endpoint apihttp.Endpoint
	log      *log.Logger
}

type apiAuthorizerHandler struct {
	apiAuthorizer
	underlying http.HandlerFunc
}

// Assigns the underlying http.HandlerFunc and returns this apiAuthorizer's
// handler func.
func (a *apiAuthorizer) wrap(underlying http.HandlerFunc) http.HandlerFunc {
	h := apiAuthorizerHandler{apiAuthorizer: *a, underlying: underlying}
	return h.handler
}

// Handles HTTP requests by sending a user-lookup request to the API.
// If the user-lookup fails, the request is treated as unauthorized.
// Otherwise, the underlying handler is invoked.
func (a *apiAuthorizerHandler) handler(rw http.ResponseWriter, r *http.Request) {
	writeError := func(err *httperr.Error) {
		rw := apihttp.RichResponseWriter{ResponseWriter: rw}
		rw.WriteEnvelope(err, nil)
	}
	apiKey, err := tbnauth.NewAPIAuthKeyFromRequest(r)
	if err != nil {
		log.Println("Authorization error: ", err)
		writeError(httperr.AuthorizationError())
		return
	}
	if tbnauth.AuthSystemForAuthKey(apiKey) != tbnauth.InternalAuthSystem {
		log.Println("Only internal authorization is supported")
		writeError(httperr.AuthorizationMethodDeniedError())
		return
	}
	apiKeyStr := string(apiKey)
	orgKey, validationError := a.validate(apiKeyStr)
	if err != nil {
		log.Println("Validation failed: ", validationError)
		writeError(httperr.AuthorizationError())
		return
	}
	requestContext := requestcontext.New(r)
	requestContext.SetOrgKey(orgKey)
	a.underlying.ServeHTTP(rw, r)
}

// Validates the given API key again the API. Returns nil if the API
// key is valid.
func (a *apiAuthorizerHandler) validate(apiKey string) (api.OrgKey, *httperr.Error) {
	svc, err := apiclient.NewAdmin(a.endpoint, apiKey)
	if err != nil {
		return "", httperr.New500(err.Error(), httperr.UnknownTransportCode)
	}

	filter := service.UserFilter{APIAuthKey: api.APIAuthKey(apiKey)}
	users, err := svc.User().Index(filter)
	if err != nil {
		return "", httperr.FromError(err, httperr.UnknownTransportCode)
	}

	if len(users) == 0 {
		log.Println("No users for API key")
		return "", httperr.AuthorizationError()
	}

	return users[0].OrgKey, nil
}

var MockAuthorizer handler.Authorizer = handler.Authorizer(
	func(wrapped http.HandlerFunc) http.HandlerFunc {
		return func(rw http.ResponseWriter, r *http.Request) {
			requestContext := requestcontext.New(r)
			requestContext.SetOrgKey(NoAuthOrgKey)
			wrapped.ServeHTTP(rw, r)
		}
	},
)
