package handler

import (
	"log"
	"net/http"

	"github.com/turbinelabs/api"
	"github.com/turbinelabs/api/service"
	svchttp "github.com/turbinelabs/api/service/http"
	clienthttp "github.com/turbinelabs/client/http"
	"github.com/turbinelabs/server/handler"
	"github.com/turbinelabs/server/header"
	httperr "github.com/turbinelabs/server/http/error"
)

type apiAuthorizer struct {
	client   *http.Client
	endpoint clienthttp.Endpoint
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
	var err *httperr.Error
	if apiKey := r.Header[header.APIKey]; apiKey != nil && len(apiKey) == 1 {
		err = a.validate(apiKey[0])
	} else {
		log.Println("Missing API key")
		err = httperr.AuthorizationError()
	}

	if err == nil {
		a.underlying.ServeHTTP(rw, r)
	} else {
		handler.RichResponseWriter{rw}.WriteEnvelope(err, nil)
	}
}

// Validates the given API key again the API. Returns nil if the API
// key is valid.
func (a *apiAuthorizerHandler) validate(apiKey string) *httperr.Error {
	svc, err := svchttp.NewAdmin(a.endpoint, apiKey, a.client)
	if err != nil {
		return httperr.New500(err.Error(), httperr.UnknownTransportCode)
	}

	filter := service.UserFilter{APIAuthKey: api.APIAuthKey(apiKey)}
	users, err := svc.User().Index(filter)
	if err != nil {
		return httperr.FromError(err, httperr.UnknownTransportCode)
	}

	if len(users) == 0 {
		log.Println("No users for API key")
		return httperr.AuthorizationError()
	}

	return nil
}
