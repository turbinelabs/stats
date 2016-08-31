package handler

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/turbinelabs/test/assert"
)

var scatterOkResponse = &http.Response{Status: "200 OK", StatusCode: 200}
var scatterFailError = errors.New("scatter/gather request failed")

var succeedingRequestFn = httpRequestFn(
	func() (*http.Response, error) {
		return scatterOkResponse, nil
	},
)

var failingRequestFn = httpRequestFn(
	func() (*http.Response, error) {
		return nil, scatterFailError
	},
)

func delayedRequestFn(delay time.Duration, f httpRequestFn) httpRequestFn {
	return func() (*http.Response, error) {
		time.Sleep(delay)
		return f()
	}
}

func TestProcessHttpQuery(t *testing.T) {
	ch := make(chan httpQueryResult, 1)

	processHttpQuery(succeedingRequestFn, ch)
	select {
	case result := <-ch:
		assert.Equal(t, result, httpQueryResult{response: scatterOkResponse})
	default:
		assert.Failed(t, "missing object on channel")
	}

	processHttpQuery(failingRequestFn, ch)
	select {
	case result := <-ch:
		assert.Equal(t, result, httpQueryResult{err: scatterFailError})
	default:
		assert.Failed(t, "missing object on channel")
	}
}

func TestScatterGatherHttpRequestsOne(t *testing.T) {
	requestFuncs := []httpRequestFn{succeedingRequestFn}

	results := scatterGatherHttpRequests(requestFuncs, 1*time.Minute)
	assert.NonNil(t, results)
	if assert.Equal(t, len(results), 1) {
		assert.Equal(t, results[0], httpQueryResult{response: scatterOkResponse})
	}
}

func TestScatterGatherHttpRequestsMultiple(t *testing.T) {
	requestFuncs := []httpRequestFn{
		succeedingRequestFn,
		failingRequestFn,
		succeedingRequestFn,
	}

	results := scatterGatherHttpRequests(requestFuncs, 1*time.Minute)
	assert.NonNil(t, results)
	if assert.Equal(t, len(results), 3) {
		assert.Equal(t, results[0], httpQueryResult{response: scatterOkResponse})
		assert.Equal(t, results[1], httpQueryResult{err: scatterFailError})
		assert.Equal(t, results[2], httpQueryResult{response: scatterOkResponse})
	}
}

func TestScatterGatherHttpRequestsMultipleSlow(t *testing.T) {
	requestFuncs := []httpRequestFn{
		delayedRequestFn(100*time.Millisecond, succeedingRequestFn),
		failingRequestFn,
		succeedingRequestFn,
	}

	results := scatterGatherHttpRequests(requestFuncs, 1*time.Minute)
	assert.NonNil(t, results)
	if assert.Equal(t, len(results), 3) {
		assert.Equal(t, results[0], httpQueryResult{response: scatterOkResponse})
		assert.Equal(t, results[1], httpQueryResult{err: scatterFailError})
		assert.Equal(t, results[2], httpQueryResult{response: scatterOkResponse})
	}
}

func TestScatterGattherHttpRequestOneTimeout(t *testing.T) {
	requestFuncs := []httpRequestFn{
		delayedRequestFn(1*time.Minute, succeedingRequestFn),
	}

	results := scatterGatherHttpRequests(requestFuncs, 100*time.Millisecond)
	assert.NonNil(t, results)
	if assert.Equal(t, len(results), 1) {
		assert.Equal(t, results[0], timeoutResult)
	}
}

func TestScatterGattherHttpRequestMultipleTimeout(t *testing.T) {
	requestFuncs := []httpRequestFn{
		succeedingRequestFn,
		delayedRequestFn(1*time.Minute, succeedingRequestFn),
		delayedRequestFn(1*time.Minute, succeedingRequestFn),
		succeedingRequestFn,
		failingRequestFn,
	}

	results := scatterGatherHttpRequests(requestFuncs, 100*time.Millisecond)
	assert.NonNil(t, results)
	if assert.Equal(t, len(results), 5) {
		assert.Equal(t, results[0], httpQueryResult{response: scatterOkResponse})
		assert.Equal(t, results[1], timeoutResult)
		assert.Equal(t, results[2], timeoutResult)
		assert.Equal(t, results[3], httpQueryResult{response: scatterOkResponse})
		assert.Equal(t, results[4], httpQueryResult{err: scatterFailError})
	}
}
