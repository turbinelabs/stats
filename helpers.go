package stats

import (
	"fmt"
	"strings"

	tbntime "github.com/turbinelabs/nonstdlib/time"
)

// Stat names for Latency, SuccessRate, and LatencyWithSuccessRate.
const (
	LatencyStat = "latency" // latency timing
	RequestStat = "request" // request count
	SuccessStat = "success" // successful request count
	FailureStat = "failure" // failed request count

	ErrorTypeTag = "error_type" // error type tag name
)

var (
	latencyTimeSource = tbntime.NewSource()
)

// Latency measures the time between its invocation and the invocation
// of the function it returns. The timing is recorded as "latency" on
// the given Stats.
func Latency(s Stats) func() {
	start := latencyTimeSource.Now()
	return func() {
		delta := latencyTimeSource.Now().Sub(start)

		// Handle clock resets with something approximating grace.
		// TODO: go1.9 may obviate this:
		// https://github.com/golang/proposal/blob/master/design/12914-monotonic.md
		if delta < 0 {
			delta = 0
		}

		s.Timing(LatencyStat, delta)
	}
}

// SanitizeErrorType converts an error's type into a format suitable for
// inclusion in a stats tag. Specifically it removes '*'.
func SanitizeErrorType(err error) string {
	return strings.Map(
		func(r rune) rune {
			if r == '*' {
				return -1
			}
			return r
		},
		fmt.Sprintf("%T", err),
	)
}

// SuccessRate counts requests, successes, and failures. Each
// invocation counts a "request". If err is nil, a "success" is
// counted. If err is non-nil, a "failure" is counted with an
// "error_type" tag indicating the error's type.
func SuccessRate(s Stats, err error) {
	s.Count(RequestStat, 1)
	if err != nil {
		s.Count(FailureStat, 1, NewKVTag(ErrorTypeTag, SanitizeErrorType(err)))
	} else {
		s.Count(SuccessStat, 1)
	}
}

// LatencyWithSuccessRate combines Latency and SuccessRate. Like
// Latency, is measures time from its invocation until the returned
// function is invoked. The returned function uses its error parameter
// to distinguish between successful and failed requests.
func LatencyWithSuccessRate(s Stats) func(error) {
	latency := Latency(s)

	return func(err error) {
		latency()
		SuccessRate(s, err)
	}
}
