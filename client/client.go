package client

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	tbnhttp "github.com/turbinelabs/client/http"
	httperr "github.com/turbinelabs/server/http/error"
	"github.com/turbinelabs/stats"
)

const (
	clientID string = "tbn-stats-client (v0.1)"
)

type Stats interface {
	Forward(context.Context, stats.StatsPayload) (stats.Result, error)
}

type httpStatsV1 struct {
	dest tbnhttp.Endpoint

	requestHandler tbnhttp.RequestHandler
}

func NewStats(
	dest tbnhttp.Endpoint,
	apiKey string,
	client *http.Client,
) (Stats, error) {
	if client == nil {
		return nil, fmt.Errorf("Attempting to configure Stats with nil *http.Client")
	}

	return &httpStatsV1{dest, tbnhttp.NewRequestHandler(client, apiKey, clientID)}, nil
}

func (hs *httpStatsV1) post(encodedBody string) (*http.Request, error) {
	rdr := strings.NewReader(encodedBody)
	return http.NewRequest("POST", hs.dest.Url("/v1.0/stats/forward", tbnhttp.Params{}), rdr)
}

func (hs *httpStatsV1) Forward(
	ctxt context.Context,
	payload stats.StatsPayload,
) (stats.Result, error) {
	var encoded string
	if b, err := json.Marshal(payload); err == nil {
		encoded = string(b)
	} else {
		msg := fmt.Sprintf("could not encode provided stats payload: %+v", payload)
		return stats.Result{}, httperr.New400(msg, httperr.UnknownEncodingCode)
	}

	reqFn := func() (*http.Request, error) {
		req, err := hs.post(encoded)
		if err == nil {
			req = req.WithContext(ctxt)
		}
		return req, err
	}
	response := stats.Result{}
	if err := hs.requestHandler.Do(reqFn, &response); err != nil {
		return stats.Result{}, err
	}

	return response, nil
}
