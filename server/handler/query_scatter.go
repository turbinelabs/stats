package handler

import (
	"errors"
	"net/http"
	"time"
)

type httpRequestFn func() (*http.Response, error)

type httpQueryResult struct {
	response *http.Response
	err      error
}

var timeoutResult = httpQueryResult{err: errors.New("request timeout")}

func processHttpQuery(requestFn httpRequestFn, ch chan httpQueryResult) {
	response, err := requestFn()
	ch <- httpQueryResult{response: response, err: err}
}

func scatterGatherHttpRequests(
	requestFuncs []httpRequestFn,
	timeout time.Duration,
) []httpQueryResult {
	numRequests := len(requestFuncs)

	channels := make([]chan httpQueryResult, numRequests)
	for i := range channels {
		channels[i] = make(chan httpQueryResult, 1)

		go processHttpQuery(requestFuncs[i], channels[i])
	}
	defer func() {
		for _, ch := range channels {
			close(ch)
		}
	}()

	timer := time.NewTimer(timeout)
	results := make([]httpQueryResult, numRequests)
	timedOut := false
	for i, ch := range channels {
		var result *httpQueryResult
		select {
		case r := <-ch:
			result = &r
		default:
			// channel not ready
		}

		if result == nil && !timedOut {
			// wait for channel ready or timeout
			select {
			case r := <-ch:
				result = &r

			case <-timer.C:
				timedOut = true
			}
		}

		if result != nil {
			results[i] = *result
		} else {
			// timeout
			results[i] = timeoutResult
		}
	}

	timer.Stop()

	return results
}
