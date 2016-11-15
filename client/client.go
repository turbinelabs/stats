package client

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	tbnhttp "github.com/turbinelabs/client/http"
	"github.com/turbinelabs/nonstdlib/executor"
	httperr "github.com/turbinelabs/server/http/error"
	"github.com/turbinelabs/stats"
)

const (
	clientID string = "tbn-stats-client (v0.1)"

	forwardPath = "/v1.0/stats/forward"
)

// Stats forwards stats data to a remote stats-server.
type Stats interface {
	// Forward the given stats payload.
	Forward(*stats.StatsPayload) (*stats.Result, error)

	// Closes the client and releases any resources it created.
	Close() error
}

// internalStats is an internal interface for issuing forwarding
// requests.
type internalStats interface {
	// Issues a forwarding request for the given payload with the
	// given executor.CallbackFunc.
	IssueRequest(*stats.StatsPayload, executor.CallbackFunc) error
}

type httpStatsV1 struct {
	dest           tbnhttp.Endpoint
	requestHandler tbnhttp.RequestHandler
	exec           executor.Executor
}

var _ Stats = &httpStatsV1{}
var _ internalStats = &httpStatsV1{}

// NewStats returns a blocking implementation of Stats. Each
// invocation of Forward accepts a single Payload, issues a forwarding
// request to a remote stats-server and awaits a response.
func NewStats(
	dest tbnhttp.Endpoint,
	apiKey string,
	client *http.Client,
	exec executor.Executor,
) (Stats, error) {
	return newInternalStats(dest, apiKey, client, exec)
}

func newInternalStats(
	dest tbnhttp.Endpoint,
	apiKey string,
	client *http.Client,
	exec executor.Executor,
) (*httpStatsV1, error) {
	if client == nil {
		return nil, fmt.Errorf("Attempting to configure Stats with nil *http.Client")
	}

	return &httpStatsV1{dest, tbnhttp.NewRequestHandler(client, apiKey, clientID), exec}, nil
}

func encodePayload(payload *stats.StatsPayload) (string, error) {
	if b, err := json.Marshal(payload); err == nil {
		return string(b), nil
	} else {
		msg := fmt.Sprintf("could not encode stats payload: %+v\n%+v", err, payload)
		return "", httperr.New400(msg, httperr.UnknownEncodingCode)
	}
}

func (hs *httpStatsV1) IssueRequest(payload *stats.StatsPayload, cb executor.CallbackFunc) error {
	encoded, err := encodePayload(payload)
	if err != nil {
		return err
	}

	hs.exec.Exec(
		func(ctxt context.Context) (interface{}, error) {
			response := &stats.Result{}
			if err := hs.requestHandler.Do(
				func() (*http.Request, error) {
					rdr := strings.NewReader(encoded)
					req, err := http.NewRequest(
						"POST",
						hs.dest.Url(forwardPath, tbnhttp.Params{}),
						rdr)
					if err != nil {
						return nil, err
					}
					return req.WithContext(ctxt), nil
				},
				response,
			); err != nil {
				return nil, err
			}

			return response, nil
		},
		cb,
	)
	return nil
}

func (hs *httpStatsV1) Forward(payload *stats.StatsPayload) (*stats.Result, error) {
	responseChan := make(chan executor.Try, 1)
	defer close(responseChan)

	err := hs.IssueRequest(
		payload,
		func(try executor.Try) {
			responseChan <- try
		},
	)
	if err != nil {
		return nil, err
	}

	try := <-responseChan
	if try.IsError() {
		return nil, try.Error()
	} else {
		return try.Get().(*stats.Result), nil
	}
}

func (hs *httpStatsV1) Close() error {
	return nil
}
