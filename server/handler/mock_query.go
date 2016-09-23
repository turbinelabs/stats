package handler

import (
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/turbinelabs/api"
	httperr "github.com/turbinelabs/server/http/error"
)

func NewMockQueryHandler() QueryHandler {
	initMockQueryData()
	return &mockQueryHandler{}
}

type mockQueryHandler struct {
	queryHandler
}

const (
	secondsPerDay    = 86400
	secondsPerHour   = 3600
	microsPerSecond  = 1000000
	baseRequestCount = 1000.0
	requestJitter    = 0.01
	maxErrorRate     = 0.015
	maxFailureRate   = 0.002
	maxLatency       = 0.1 * microsPerSecond // 100 ms
)

// Coefficients for a polynomial representing something like diurnal
// request rate variation; y ranges from [1.0, 2.0] for x [0, 86.4]
// (kiloseconds). (Suitable for pasting into Grapher.)
// y = -4.446703031 ⋅ 10^{-14} ⋅ x^{9}
//   +  1.606422852 ⋅ 10^{-11} ⋅ x^{8}
//   + -2.394952041 ⋅ 10^{-9} ⋅ x^{7}
//   +  1.910900105 ⋅ 10^{-7} ⋅ x^{6}
//   + -8.873888532 ⋅ 10^{-6} ⋅ x^{5}
//   +  2.449571785 ⋅ 10^{-4} ⋅ x^{4}
//   + -3.927898101 ⋅ 10^{-3} ⋅ x^{3}
//   +  3.380314898 ⋅ 10^{-2} ⋅ x^{2}
//   + -1.384811916 ⋅ 10^{-1} ⋅ x
//   +  1.49988122
var coeffs = []float64{
	-4.446703031,
	+1.606422852,
	-2.394952041,
	+1.910900105,
	-8.873888532,
	+2.449571785,
	-3.927898101,
	+3.380314898,
	-1.384811916,
	+1.49988122,
}

var pows = []int{
	-14,
	-11,
	-9,
	-7,
	-6,
	-4,
	-3,
	-2,
	-1,
	0,
}

var (
	mockInitialized  bool
	mockRequests     []float64
	mockErrorRates   []float64
	mockFailureRates []float64
)

func initMockQueryData() {
	if mockInitialized {
		return
	}

	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)

	mockRequests = make([]float64, 86401)
	mockErrorRates = make([]float64, secondsPerHour)
	mockFailureRates = make([]float64, secondsPerHour)

	for s := 0; s <= secondsPerDay; s++ {
		d := diurnalFactor(s)
		j := rng.NormFloat64()*requestJitter + 1.0
		requests := d * j
		mockRequests[s] = requests * baseRequestCount
	}

	for s := 0; s < secondsPerHour; s++ {
		mockErrorRates[s] = rng.Float64() * maxErrorRate
		mockFailureRates[s] = rng.Float64() * maxFailureRate
	}

	mockInitialized = true
}

// Returns a value in the range [1.0, 2.0] based on the number of
// seconds since midnight ([0, 86400]).
func diurnalFactor(s int) float64 {
	x := float64(s) / 1000.0
	i := 0
	var r float64
	for i < len(coeffs) {
		xpow := float64(len(pows) - i - 1)
		r += math.Pow(x, xpow) * coeffs[i] * math.Pow10(pows[i])
		i++
	}
	return r
}

func numRequests(ts int64) float64 {
	s := (ts / microsPerSecond) % secondsPerDay
	return math.Floor(mockRequests[s] + 0.5)
}

func numFailures(ts int64) float64 {
	// seconds offset within day
	s := (ts / microsPerSecond) % secondsPerDay

	r := mockRequests[s]

	// take the hour within the day and offset into the hours
	// worth of failure rates (3600 / 24 = 150)
	offset := (s / secondsPerHour) * 150

	// pick the rate
	idx := (s + offset) % secondsPerHour

	return math.Floor(mockFailureRates[idx]*r + 0.5)
}

func numErrors(ts int64) float64 {
	// seconds offset within day
	s := (ts / microsPerSecond) % secondsPerDay

	r := mockRequests[s]

	// take the hour within the day and offset into the hours
	// worth of failure rates (3600 / 24 = 150)
	offset := (s / secondsPerHour) * 150

	// pick the rate
	idx := (s + offset) % secondsPerHour

	return math.Floor(mockErrorRates[idx]*r + 0.5)
}

func mockCountTimeSeries(start, end int64, qts StatsQueryTimeSeries) StatsTimeSeries {
	numPoints := (end - start) / microsPerSecond
	points := make([]StatsPoint, numPoints)
	for idx := int64(0); idx < numPoints; idx++ {
		ts := start + (idx * microsPerSecond)

		points[idx].Timestamp = ts

		var value float64
		switch qts.QueryType {
		case Requests:
			value = numRequests(ts)

		case Responses:
			value = numRequests(ts) - numFailures(ts)

		case SuccessfulResponses:
			value = numRequests(ts) - numFailures(ts) - numErrors(ts)

		case ErrorResponses:
			value = numErrors(ts)

		case FailureResponses:
			value = numFailures(ts)

		default:
			value = 1.0
		}

		points[idx].Value = value
	}

	return StatsTimeSeries{Query: qts, Points: points}
}

func pickLatency(ts int64, percentile float64) float64 {
	// cumulative distribution: 1-e^(-λx) ; λ = 1.5
	x := numRequests(ts) / (baseRequestCount * (1.0 + requestJitter)) * percentile
	d := 1.0 - math.Exp(-1.0*x)
	return math.Floor(maxLatency*d + 0.5)
}

func mockLatencyTimeSeries(start, end int64, qts StatsQueryTimeSeries) StatsTimeSeries {
	numPoints := (end - start) / microsPerSecond
	points := make([]StatsPoint, numPoints)

	for idx := int64(0); idx < numPoints; idx++ {
		ts := start + (idx * microsPerSecond)

		points[idx].Timestamp = ts

		var value float64
		switch qts.QueryType {
		case LatencyP50:
			value = pickLatency(ts, .5)

		case LatencyP99:
			value = pickLatency(ts, .99)

		default:
			value = 0.0
		}

		points[idx].Value = value
	}

	return StatsTimeSeries{Query: qts, Points: points}
}

func (mqh *mockQueryHandler) RunQuery(
	orgKey api.OrgKey,
	q StatsQuery,
) (*StatsQueryResult, *httperr.Error) {
	if err := validateQuery(&q); err != nil {
		return nil, err
	}

	start, end, err := normalizeTimeRange(q.TimeRange)
	if err != nil {
		return nil, err
	}

	duration := end - start
	result := StatsQueryResult{
		TimeRange: StatsTimeRange{
			Start:    &start,
			End:      &end,
			Duration: &duration,
		},
		TimeSeries: make([]StatsTimeSeries, len(q.TimeSeries)),
	}

	for idx, qts := range q.TimeSeries {
		switch qts.QueryType {
		case Requests, Responses, SuccessfulResponses, ErrorResponses, FailureResponses:
			result.TimeSeries[idx] = mockCountTimeSeries(start, end, qts)
		case LatencyP50, LatencyP99:
			result.TimeSeries[idx] = mockLatencyTimeSeries(start, end, qts)

		default:
			err = httperr.New500(
				fmt.Sprintf("unknown query type %s", qts.QueryType),
				httperr.UnknownUnclassifiedCode,
			)
			return nil, err
		}
	}

	return &result, nil
}

func (mqh *mockQueryHandler) AsHandler() http.HandlerFunc {
	return mkHandlerFunc(mqh)
}
