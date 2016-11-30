package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	apihttp "github.com/turbinelabs/api/http"
	"github.com/turbinelabs/api/http/envelope"
	httperr "github.com/turbinelabs/api/http/error"
	apiheader "github.com/turbinelabs/api/http/header"
	statsapi "github.com/turbinelabs/api/service/stats"
	"github.com/turbinelabs/logparser/metric"
	"github.com/turbinelabs/nonstdlib/executor"
	tbntime "github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/test/assert"
)

var (
	sourceString1 = "the-source"

	source1, _ = metric.NewSource(sourceString1, "")

	metricName1 = "group.metric"
	metric1, _  = source1.NewMetric(metricName1)

	when1       = time.Now()
	when1Micros = tbntime.ToUnixMicro(when1)

	payload = &statsapi.Payload{
		Source: sourceString1,
		Stats: []statsapi.Stat{
			{
				Name:      metric1.Name(),
				Value:     1.41421,
				Timestamp: when1Micros,
				Tags:      map[string]string{"tag": "tag-value"},
			},
		},
	}

	badPayload = &statsapi.Payload{
		Source: sourceString1,
		Stats: []statsapi.Stat{
			{
				Name:      metric1.Name(),
				Value:     math.Inf(1),
				Timestamp: when1Micros,
				Tags:      map[string]string{},
			},
		},
	}

	testApiKey = "i.am.a.key"

	endpoint, _ = apihttp.NewEndpoint(apihttp.HTTP, "example.com", 8080)
)

func TestEncodePayload(t *testing.T) {
	json, err := encodePayload(payload)
	assert.Nil(t, err)
	assert.Equal(
		t,
		json,
		fmt.Sprintf(
			`{"source":"%s","stats":[{"name":"%s","value":%g,"timestamp":%d,"tags":{"%s":"%s"}}]}`,
			sourceString1,
			metricName1,
			1.41421,
			when1Micros,
			"tag",
			"tag-value",
		),
	)
}

func TestEncodePayloadError(t *testing.T) {
	json, err := encodePayload(badPayload)
	assert.Equal(t, json, "")
	assert.NonNil(t, err)
}

type forwardResult struct {
	result *statsapi.ForwardResult
	err    error
}

type resultFunc func() (*statsapi.ForwardResult, error)
type requestFunc func(statsapi.StatsService) (*statsapi.ForwardResult, error)
type newStatsFunc func(
	apihttp.Endpoint,
	string,
	executor.Executor,
) (statsapi.StatsService, error)

func prepareStatsClientTest(
	t *testing.T,
	e apihttp.Endpoint,
	reqFunc requestFunc,
) (executor.Func, executor.CallbackFunc, resultFunc) {
	ctrl := gomock.NewController(assert.Tracing(t))

	funcChan := make(chan executor.Func, 1)
	callbackFuncChan := make(chan executor.CallbackFunc, 1)

	mockExec := executor.NewMockExecutor(ctrl)
	mockExec.EXPECT().
		Exec(gomock.Any(), gomock.Any()).
		Do(
			func(f executor.Func, cb executor.CallbackFunc) {
				funcChan <- f
				callbackFuncChan <- cb
			},
		)

	client, err := NewStatsClient(e, testApiKey, mockExec)
	assert.Nil(t, err)

	rvChan := make(chan forwardResult, 1)

	go func() {
		r, err := reqFunc(client)
		rvChan <- forwardResult{r, err}
	}()

	f := <-funcChan
	cb := <-callbackFuncChan

	return f, cb, func() (*statsapi.ForwardResult, error) {
		defer ctrl.Finish()
		rv := <-rvChan
		return rv.result, rv.err
	}
}

func payloadForward(p *statsapi.Payload) func(client statsapi.StatsService) (*statsapi.ForwardResult, error) {
	return func(client statsapi.StatsService) (*statsapi.ForwardResult, error) {
		return client.Forward(p)
	}
}

var simpleForward = payloadForward(payload)

func TestStatsClientForward(t *testing.T) {
	_, cb, getResult := prepareStatsClientTest(t, endpoint, simpleForward)

	expectedResult := &statsapi.ForwardResult{NumAccepted: 1}
	cb(executor.NewReturn(expectedResult))

	result, err := getResult()
	assert.SameInstance(t, result, expectedResult)
	assert.Nil(t, err)
}

func TestStatsClientForwardFailure(t *testing.T) {
	_, cb, getResult := prepareStatsClientTest(t, endpoint, simpleForward)

	expectedErr := errors.New("failure")
	cb(executor.NewError(expectedErr))

	result, err := getResult()
	assert.Nil(t, result)
	assert.SameInstance(t, err, expectedErr)
}

type testHandler struct {
	t               *testing.T
	requestPayload  *statsapi.Payload
	responsePayload *statsapi.ForwardResult
	responseError   *httperr.Error
}

func (h *testHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	apiKey := req.Header.Get(http.CanonicalHeaderKey(apiheader.APIKey))
	if apiKey != testApiKey {
		resp.WriteHeader(400)
		resp.Write([]byte(
			fmt.Sprintf(
				"wrong api key header, got %s, want %s",
				apiKey,
				testApiKey,
			),
		))
		return
	}

	clientId := req.Header.Get(http.CanonicalHeaderKey(apiheader.ClientID))
	if clientId != statsClientID {
		resp.WriteHeader(400)
		resp.Write([]byte(
			fmt.Sprintf(
				"wrong client id header: got %s, want %s",
				clientId,
				statsClientID,
			),
		))
		return
	}

	body := req.Body
	assert.NonNil(h.t, body)

	bytes, err := ioutil.ReadAll(body)
	defer body.Close()
	assert.Nil(h.t, err)

	stats := &statsapi.Payload{}
	err = json.Unmarshal(bytes, stats)
	assert.Nil(h.t, err)
	h.requestPayload = stats

	envelope := &envelope.Response{Error: h.responseError, Payload: h.responsePayload}
	bytes, err = json.Marshal(envelope)
	assert.Nil(h.t, err)

	resp.WriteHeader(200)
	resp.Write(bytes)
}

func runStatsClientFuncTest(
	t *testing.T,
	requestPayload *statsapi.Payload,
	responsePayload *statsapi.ForwardResult,
	httpErr *httperr.Error,
) (*statsapi.Payload, *statsapi.ForwardResult, error) {
	handler := &testHandler{responsePayload: responsePayload, responseError: httpErr}
	server := httptest.NewServer(handler)
	defer server.Close()

	host, portStr, _ := net.SplitHostPort(server.Listener.Addr().String())
	port, _ := net.LookupPort(server.Listener.Addr().Network(), portStr)

	endpoint, err := apihttp.NewEndpoint(apihttp.HTTP, host, port)
	assert.Nil(t, err)

	f, cb, _ := prepareStatsClientTest(t, endpoint, payloadForward(requestPayload))
	cb(executor.NewError(errors.New("don't care about this")))

	ctxt := context.Background()

	response, err := f(ctxt)
	if response == nil {
		return handler.requestPayload, nil, err
	}
	return handler.requestPayload, response.(*statsapi.ForwardResult), err
}

func TestStatsClientForwardExecFunc(t *testing.T) {
	expectedResult := &statsapi.ForwardResult{NumAccepted: 12}

	gotPayload, result, err := runStatsClientFuncTest(t, payload, expectedResult, nil)
	assert.DeepEqual(t, gotPayload, payload)
	assert.DeepEqual(t, result, expectedResult)
	assert.Nil(t, err)
}

func TestStatsClientForwardExecFuncFailure(t *testing.T) {
	expectedErr := httperr.AuthorizationError()

	gotPayload, result, err := runStatsClientFuncTest(t, payload, nil, expectedErr)
	assert.DeepEqual(t, gotPayload, payload)
	assert.Nil(t, result)
	assert.Equal(t, err.Error(), expectedErr.Error())
}
