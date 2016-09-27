package handler

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/turbinelabs/api"
	"github.com/turbinelabs/test/assert"
)

type formatMetricTestCase struct {
	orgKey    api.OrgKey
	zoneKey   api.ZoneKey
	domainKey *api.DomainKey
	routeKey  *api.RouteKey
	method    *string
	queryType QueryType

	expectedMetric string
}

func (tc formatMetricTestCase) run(t *testing.T) {
	metric := formatMetric(
		tc.orgKey,
		tc.zoneKey,
		tc.domainKey,
		tc.routeKey,
		tc.method,
		tc.queryType,
	)
	assert.Equal(t, metric, tc.expectedMetric)
}

func TestFormatMetric(t *testing.T) {
	ok := api.OrgKey("o")
	zk := api.ZoneKey("z")
	dk := api.DomainKey("d")
	rk := api.RouteKey("r")
	md := "POST"
	testCases := []formatMetricTestCase{
		{ok, zk, nil, nil, nil, Requests, "o.z.*.*.*.requests"},
		{ok, zk, &dk, nil, nil, Requests, "o.z.d.*.*.requests"},
		{ok, zk, nil, &rk, nil, Requests, "o.z.*.r.*.requests"},
		{ok, zk, nil, nil, &md, Requests, "o.z.*.*.POST.requests"},
		{ok, zk, &dk, &rk, nil, Requests, "o.z.d.r.*.requests"},
		{ok, zk, &dk, nil, &md, Requests, "o.z.d.*.POST.requests"},
		{ok, zk, nil, &rk, &md, Requests, "o.z.*.r.POST.requests"},
		{ok, zk, &dk, &rk, &md, Requests, "o.z.d.r.POST.requests"},
		{ok, zk, nil, nil, nil, Responses, "o.z.*.*.*.responses"},
	}

	for _, tc := range testCases {
		tc.run(t)
	}
}

type formatQueryTestCase struct {
	metric       string
	clusterKey   *api.ClusterKey
	instanceKeys []string

	expectedQuery string
}

func (tc formatQueryTestCase) run(t *testing.T) {
	qts := StatsQueryTimeSeries{
		ClusterKey:   tc.clusterKey,
		InstanceKeys: tc.instanceKeys,
	}

	query := formatQuery(tc.metric, &qts)
	assert.Equal(t, query, tc.expectedQuery)
}

func TestFormatQuery(t *testing.T) {
	ck := api.ClusterKey("c")
	ik1 := []string{"i1"}
	ik2 := []string{"i1", "i2"}
	m := "a-metric"
	testCases := []formatQueryTestCase{
		{m, nil, nil, `ts("a-metric")`},
		{m, &ck, nil, `ts("a-metric", upstream="c")`},
		{m, nil, ik1, `ts("a-metric", instance="i1")`},
		{m, nil, ik2, `ts("a-metric", instance="i1" or instance="i2")`},
		{m, &ck, ik1, `ts("a-metric", upstream="c" and (instance="i1"))`},
		{m, &ck, ik2, `ts("a-metric", upstream="c" and (instance="i1" or instance="i2"))`},
	}

	for _, tc := range testCases {
		tc.run(t)
	}
}

func TestFormatWavefrontQueryUrl(t *testing.T) {
	start := int64(1472667004)
	end := start + 3600
	orgKey := api.OrgKey("o")
	zoneKey := api.ZoneKey("z")
	domainKey := api.DomainKey("d")
	routeKey := api.RouteKey("r")
	method := "GET"

	qts := StatsQueryTimeSeries{
		QueryType: Requests,
		DomainKey: &domainKey,
		RouteKey:  &routeKey,
		Method:    &method,
	}

	u := formatWavefrontQueryUrl(start*1000000, end*1000000, orgKey, zoneKey, &qts)
	url, err := url.Parse(u)
	assert.Nil(t, err)

	assert.Equal(t, url.Scheme, "https")
	assert.Equal(t, url.Host, "metrics.wavefront.com")
	assert.Equal(t, url.Path, "/chart/api")

	queryParams := url.Query()
	for k, v := range queryParams {
		if !assert.Equal(t, len(v), 1) {
			fmt.Println("multiple values for ", k)
		}
	}

	assert.Equal(t, queryParams.Get("g"), "s")
	assert.Equal(t, queryParams.Get("summarization"), "MEAN")
	assert.Equal(t, queryParams.Get("s"), fmt.Sprintf("%d", start))
	assert.Equal(t, queryParams.Get("e"), fmt.Sprintf("%d", end))
	assert.Equal(
		t,
		queryParams.Get("q"),
		formatQuery(
			formatMetric(orgKey, zoneKey, &domainKey, &routeKey, &method, Requests),
			&qts,
		),
	)
}

func TestFormatWavefrontQueryUrlSuccessRate(t *testing.T) {
	start := int64(1472667004)
	end := start + 3600
	orgKey := api.OrgKey("o")
	zoneKey := api.ZoneKey("z")
	domainKey := api.DomainKey("d")
	routeKey := api.RouteKey("r")
	method := "GET"

	qts := StatsQueryTimeSeries{
		QueryType: SuccessRate,
		DomainKey: &domainKey,
		RouteKey:  &routeKey,
		Method:    &method,
	}

	u := formatWavefrontQueryUrl(start*1000000, end*1000000, orgKey, zoneKey, &qts)
	url, err := url.Parse(u)
	assert.Nil(t, err)

	assert.Equal(t, url.Scheme, "https")
	assert.Equal(t, url.Host, "metrics.wavefront.com")
	assert.Equal(t, url.Path, "/chart/api")

	queryParams := url.Query()
	for k, v := range queryParams {
		if !assert.Equal(t, len(v), 1) {
			fmt.Println("multiple values for ", k)
		}
	}

	assert.Equal(t, queryParams.Get("g"), "s")
	assert.Equal(t, queryParams.Get("summarization"), "MEAN")
	assert.Equal(t, queryParams.Get("s"), fmt.Sprintf("%d", start))
	assert.Equal(t, queryParams.Get("e"), fmt.Sprintf("%d", end))
	assert.Equal(
		t,
		queryParams.Get("q"),
		queryExprMap[SuccessRate].Format(orgKey, zoneKey, &qts),
	)
}

type formatQueryExprTestCase struct {
	queryType     QueryType
	expectedQuery string
}

func (tc formatQueryExprTestCase) run(t *testing.T, orgKey api.OrgKey, zoneKey api.ZoneKey, qts StatsQueryTimeSeries) {
	qts.QueryType = tc.queryType

	expr := queryExprMap[qts.QueryType]

	query := expr.Format(orgKey, zoneKey, &qts)
	assert.Equal(t, query, tc.expectedQuery)
}

func TestFormatQueryExprs(t *testing.T) {
	ok := api.OrgKey("o")
	zk := api.ZoneKey("z")

	successfulResponsesQuery :=
		`(ts("o.z.*.*.*.responses.1*")+ts("o.z.*.*.*.responses.2*")+ts("o.z.*.*.*.responses.3*"))`
	testCases := []formatQueryExprTestCase{
		{Requests, `ts("o.z.*.*.*.requests")`},
		{Responses, `ts("o.z.*.*.*.responses")`},
		{SuccessfulResponses, successfulResponsesQuery},
		{ErrorResponses, `ts("o.z.*.*.*.responses.4*")`},
		{FailureResponses, `ts("o.z.*.*.*.responses.5*")`},
		{LatencyP50, `ts("o.z.*.*.*.latency_p50")`},
		{LatencyP99, `ts("o.z.*.*.*.latency_p99")`},
		{SuccessRate, `(` + successfulResponsesQuery + `/ts("o.z.*.*.*.requests"))`},
	}

	for _, tc := range testCases {
		tc.run(t, ok, zk, StatsQueryTimeSeries{})
	}
}

func TestFormatQueryExprWithTags(t *testing.T) {
	ok := api.OrgKey("o")
	zk := api.ZoneKey("z")
	ck := api.ClusterKey("c")
	qts := StatsQueryTimeSeries{
		ClusterKey:   &ck,
		InstanceKeys: []string{"i1"},
	}
	formatQueryExprTestCase{
		Requests,
		`ts("o.z.*.*.*.requests", upstream="c" and (instance="i1"))`,
	}.run(t, ok, zk, qts)
}

const wavefrontResponse = `{
  "query": "ts(stats.counters.api.requests.count)",
  "name": "ts(stats.counters.api.requests.count)",
  "timeseries": [
    {
      "label": "stats.counters.api.requests.count",
      "host": "statsd",
      "data": [
        [ 1472667403, 0 ],
        [ 1472667405, 0 ],
        [ 1472667411, 0 ],
        [ 1472667412, 0 ],
        [ 1472667413, 116 ],
        [ 1472667421, 0 ],
        [ 1472667425, 0 ],
        [ 1472667431, 0 ],
        [ 1472667432, 0 ],
        [ 1472667433, 0 ],
        [ 1472667435, 0 ],
        [ 1472667441, 0 ],
        [ 1472667442, 0 ],
        [ 1472667443, 116 ],
        [ 1472667445, 0 ],
        [ 1472667451, 0 ],
        [ 1472667452, 0 ],
        [ 1472667453, 1 ],
        [ 1472667455, 0 ],
        [ 1472667461, 0 ],
        [ 1472667462, 0 ],
        [ 1472667463, 0 ],
        [ 1472667465, 0 ],
        [ 1472667471, 0 ],
        [ 1472667472, 112 ]
      ]
    }
  ],
  "granularity": 1,
  "stats": {
    "keys": 25,
    "points": 25,
    "summaries": 0,
    "buffer_keys": 1764,
    "compacted_keys": 0,
    "compacted_points": 0,
    "latency": 0,
    "queries": 3,
    "s3_keys": 0,
    "cpu_ns": 8744859,
    "skipped_compacted_keys": 1642,
    "cached_compacted_keys": 0,
    "query_tasks": 0
  }
}
`

const wavefrontMalformedPointResponse = `{
  "timeseries": [
    {
      "label": "stats.counters.api.requests.count",
      "host": "statsd",
      "data": [
        [ 1472667405, 0, 100 ]
      ]
    }
  ]
}
`

func TestDecodeWavefrontResponse(t *testing.T) {
	response := &http.Response{
		Body: ioutil.NopCloser(strings.NewReader(wavefrontResponse)),
	}

	sts, err := decodeWavefrontResponse(response)
	assert.Nil(t, err)
	assert.NonNil(t, sts.Points)
	assert.Equal(t, len(sts.Points), 25)
}

func TestDecodeWavefrontInvalidResponse(t *testing.T) {
	response := &http.Response{
		Body: ioutil.NopCloser(strings.NewReader("that's not json!")),
	}

	sts, err := decodeWavefrontResponse(response)
	assert.NonNil(t, err)
	assert.Nil(t, sts.Points)
}

func TestDecodeWavefrontNoBody(t *testing.T) {
	response := &http.Response{}
	sts, err := decodeWavefrontResponse(response)
	assert.NonNil(t, err)
	assert.Nil(t, sts.Points)
}

func TestDecodeWavefrontEmptyResponse(t *testing.T) {
	response := &http.Response{
		Body: ioutil.NopCloser(strings.NewReader("")),
	}

	sts, err := decodeWavefrontResponse(response)
	assert.NonNil(t, err)
	assert.Nil(t, sts.Points)
}

func TestDecodeWavefrontExtraData(t *testing.T) {
	response := &http.Response{
		Body: ioutil.NopCloser(strings.NewReader(wavefrontResponse + `{"extra": "json"}`)),
	}

	sts, err := decodeWavefrontResponse(response)
	assert.NonNil(t, err)
	assert.Nil(t, sts.Points)
}

func TestDecodeWavefrontMalformedPoint(t *testing.T) {
	response := &http.Response{
		Body: ioutil.NopCloser(strings.NewReader(wavefrontMalformedPointResponse)),
	}

	sts, err := decodeWavefrontResponse(response)
	assert.Nil(t, err)
	assert.Equal(t, len(sts.Points), 1)
}
