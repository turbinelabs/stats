package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/turbinelabs/api"
	"github.com/turbinelabs/api/fixture"
	apihttp "github.com/turbinelabs/api/http"
	"github.com/turbinelabs/api/http/envelope"
	httperr "github.com/turbinelabs/api/http/error"
	"github.com/turbinelabs/api/http/header"
	statsapi "github.com/turbinelabs/api/service/stats"
	"github.com/turbinelabs/stats/server/handler/requestcontext"
	"github.com/turbinelabs/test/assert"
)

var fixtures = fixture.DataFixtures

func mkServer(handler http.Handler) *httptest.Server {
	return httptest.NewServer(handler)
}

func mkAuthorizer(t *testing.T, server *httptest.Server) apiAuthorizer {
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	hostPortPair := strings.Split(u.Host, ":")
	host := hostPortPair[0]
	port, err := strconv.Atoi(hostPortPair[1])
	if err != nil {
		t.Fatal(err)
	}

	return mkAuthorizerFromHostPort(t, host, port)
}

func mkAuthorizerFromHostPort(t *testing.T, host string, port int) apiAuthorizer {
	e, err := apihttp.NewEndpoint(apihttp.HTTP, host, port)
	if err != nil {
		t.Fatal(err)
	}

	logger := log.New(os.Stderr, "", log.LstdFlags)

	return apiAuthorizer{
		client:   http.DefaultClient,
		endpoint: e,
		log:      logger,
	}
}

func mkAuthorizerHandler(
	t *testing.T,
	server *httptest.Server,
	handler http.HandlerFunc,
) apiAuthorizerHandler {
	auth := mkAuthorizer(t, server)

	return apiAuthorizerHandler{
		apiAuthorizer: auth,
		underlying:    handler,
	}
}

func mkAuthorizerHandlerFromHostPort(
	t *testing.T,
	host string,
	port int,
	handler http.HandlerFunc,
) apiAuthorizerHandler {
	auth := mkAuthorizerFromHostPort(t, host, port)

	return apiAuthorizerHandler{
		apiAuthorizer: auth,
		underlying:    handler,
	}
}

type mockHandler struct {
	requests []*http.Request

	responseErr     *httperr.Error
	responsePayload interface{}
}

func (m *mockHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	m.requests = append(m.requests, r)
	apihttp.RichResponseWriter{rw}.WriteEnvelope(m.responseErr, m.responsePayload)
}

func TestAPIAuthorizerHandlerValidateSuccess(t *testing.T) {
	user := fixtures.User1
	assert.NotEqual(t, user.OrgKey, api.OrgKey(""))

	apiHandler := mockHandler{responsePayload: api.Users{user}}

	server := mkServer(&apiHandler)
	auth := mkAuthorizerHandler(t, server, nil)

	apiKey, err := auth.validate(string(user.APIAuthKey))
	assert.Equal(t, apiKey, user.OrgKey)
	assert.Nil(t, err)
}

func TestAPIAuthorizerHandlerValidateUserIndexError(t *testing.T) {
	apiHandler := mockHandler{
		responseErr: httperr.New500("reasons", httperr.UnknownUnclassifiedCode),
	}

	server := mkServer(&apiHandler)
	auth := mkAuthorizerHandler(t, server, nil)

	orgKey, err := auth.validate("123")
	assert.Equal(t, orgKey, api.OrgKey(""))
	assert.NonNil(t, err)
	assert.Equal(t, err.Status, 500)
	assert.Equal(t, err.Code, httperr.UnknownUnclassifiedCode)
	assert.Equal(t, err.Message, "reasons")
}

func TestAPIAuthorizerHandlerValidateTransportError(t *testing.T) {
	auth := mkAuthorizerHandlerFromHostPort(t, "localhost", 1, nil)

	orgKey, err := auth.validate("123")
	assert.Equal(t, orgKey, api.OrgKey(""))
	assert.NonNil(t, err)
	assert.Equal(t, err.Status, 400) // you'd expect a 500: see apiserver/http.requestHandler
	assert.Equal(t, err.Code, httperr.UnknownTransportCode)
	assert.NotEqual(t, err.Message, "")
}

func TestAPIAuthorizerHandlerValidateSuccessWithNoUsers(t *testing.T) {
	apiHandler := mockHandler{responsePayload: api.Users{}}

	server := mkServer(&apiHandler)
	auth := mkAuthorizerHandler(t, server, nil)

	orgKey, err := auth.validate("123")
	assert.Equal(t, orgKey, api.OrgKey(""))
	assert.NonNil(t, err)
	assert.Equal(t, err.Status, 403)
	assert.Equal(t, err.Code, httperr.UnknownUnauthorizedCode)
	assert.NotEqual(t, err.Message, "")
}

func TestApiAuthorizerHandlerSuccess(t *testing.T) {
	user := fixtures.User1
	apiHandler := mockHandler{responsePayload: api.Users{user}}

	payload := &statsapi.ForwardResult{NumAccepted: 0}
	underlyingHandler := mockHandler{responsePayload: payload}

	server := mkServer(&apiHandler)
	auth := mkAuthorizer(t, server)

	request, err := http.NewRequest("GET", "/whatever", nil)
	assert.Nil(t, err)
	request.Header.Add(header.APIKey, string(user.APIAuthKey))

	responseRecorder := httptest.NewRecorder()

	authHandler := auth.wrap(underlyingHandler.ServeHTTP)
	authHandler(responseRecorder, request)

	assert.Equal(t, responseRecorder.Code, 200)

	rawResponsePayload := json.RawMessage{}
	response := &envelope.Response{Payload: &rawResponsePayload}
	err = json.Unmarshal(responseRecorder.Body.Bytes(), response)
	assert.Nil(t, err)
	assert.Nil(t, response.Error)

	responsePayload := &statsapi.ForwardResult{}
	err = json.Unmarshal(rawResponsePayload, responsePayload)

	assert.DeepEqual(t, responsePayload, payload)

	requestContext := requestcontext.New(request)
	rcOrgKey, ok := requestContext.GetOrgKey()
	assert.Equal(t, rcOrgKey, user.OrgKey)
	assert.True(t, ok)
}

func TestApiAuthorizerHandlerWrongAuthSystem(t *testing.T) {
	auth := apiAuthorizer{}

	failingHandler := func(rw http.ResponseWriter, r *http.Request) {
		t.Error("unexpected invocation of handler")
		rw.WriteHeader(500)
		rw.Write([]byte("nope"))
	}

	request, err := http.NewRequest("GET", "/whatever", nil)
	assert.Nil(t, err)
	request.Header.Add(header.APIKey, "Bearer xyz")

	responseRecorder := httptest.NewRecorder()

	handler := auth.wrap(failingHandler)
	handler(responseRecorder, request)

	assert.Equal(t, responseRecorder.Code, 403)

	response := &envelope.Response{}
	err = json.Unmarshal(responseRecorder.Body.Bytes(), response)
	assert.Nil(t, err)
	assert.DeepEqual(t, response.Error.Code, httperr.AuthMethodDeniedCode)

	assert.NonNil(t, response.Error)
	assert.Nil(t, response.Payload)

	requestContext := requestcontext.New(request)
	rcOrgKey, ok := requestContext.GetOrgKey()
	assert.Equal(t, rcOrgKey, api.OrgKey(""))
	assert.False(t, ok)
}

func TestApiAuthorizerHandlerNoHeader(t *testing.T) {
	auth := apiAuthorizer{}

	failingHandler := func(rw http.ResponseWriter, r *http.Request) {
		t.Error("unexpected invocation of handler")
		rw.WriteHeader(500)
		rw.Write([]byte("nope"))
	}

	request, err := http.NewRequest("GET", "/whatever", nil)
	assert.Nil(t, err)

	responseRecorder := httptest.NewRecorder()

	handler := auth.wrap(failingHandler)
	handler(responseRecorder, request)

	assert.Equal(t, responseRecorder.Code, 403)

	response := &envelope.Response{}
	err = json.Unmarshal(responseRecorder.Body.Bytes(), response)
	assert.Nil(t, err)

	assert.NonNil(t, response.Error)
	assert.Nil(t, response.Payload)

	requestContext := requestcontext.New(request)
	rcOrgKey, ok := requestContext.GetOrgKey()
	assert.Equal(t, rcOrgKey, api.OrgKey(""))
	assert.False(t, ok)
}

func TestMockAuthorizer(t *testing.T) {
	handler := func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(200)
		rw.Write([]byte("OK"))
	}

	request, err := http.NewRequest("GET", "/whatever", nil)
	assert.Nil(t, err)

	responseRecorder := httptest.NewRecorder()

	wrappedHandler := MockAuthorizer(handler)
	wrappedHandler(responseRecorder, request)

	assert.Equal(t, responseRecorder.Code, 200)
	assert.Equal(t, responseRecorder.Body.String(), "OK")

	requestContext := requestcontext.New(request)
	rcOrgKey, _ := requestContext.GetOrgKey()
	assert.Equal(t, rcOrgKey, api.OrgKey(NoAuthOrgKey))
}
