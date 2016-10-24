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
	orgKey     api.OrgKey
	zoneName   string
	domainHost *string
	routeKey   *api.RouteKey
	method     *string
	queryType  QueryType

	expectedMetric string
}

func (tc formatMetricTestCase) run(t *testing.T) {
	metric := formatMetric(
		tc.orgKey,
		tc.zoneName,
		tc.domainHost,
		tc.routeKey,
		tc.method,
		tc.queryType,
	)
	assert.Equal(t, metric, tc.expectedMetric)
}

func TestFormatMetric(t *testing.T) {
	ok := api.OrgKey("o")
	zn := "z"
	dh := "d"
	dp := "d:443"
	rk := api.RouteKey("r")
	md := "POST"
	testCases := []formatMetricTestCase{
		{ok, zn, nil, nil, nil, Requests, "o.z.*.*.*.requests"},
		{ok, zn, &dh, nil, nil, Requests, "o.z.d_*.*.*.requests"},
		{ok, zn, nil, &rk, nil, Requests, "o.z.*.r.*.requests"},
		{ok, zn, nil, nil, &md, Requests, "o.z.*.*.POST.requests"},
		{ok, zn, &dh, &rk, nil, Requests, "o.z.d_*.r.*.requests"},
		{ok, zn, &dh, nil, &md, Requests, "o.z.d_*.*.POST.requests"},
		{ok, zn, nil, &rk, &md, Requests, "o.z.*.r.POST.requests"},
		{ok, zn, &dh, &rk, &md, Requests, "o.z.d_*.r.POST.requests"},
		{ok, zn, nil, nil, nil, Responses, "o.z.*.*.*.responses"},

		{ok, zn, &dp, nil, nil, Requests, "o.z.d_443.*.*.requests"},
		{ok, zn, &dp, &rk, nil, Requests, "o.z.d_443.r.*.requests"},
		{ok, zn, &dp, nil, &md, Requests, "o.z.d_443.*.POST.requests"},
		{ok, zn, &dp, &rk, &md, Requests, "o.z.d_443.r.POST.requests"},
	}

	for _, tc := range testCases {
		tc.run(t)
	}
}

type formatQueryTestCase struct {
	metric         string
	ruleKey        *api.RuleKey
	sharedRuleName *string
	clusterName    *string
	instanceKeys   []string

	expectedQuery string
}

func (tc formatQueryTestCase) run(t *testing.T) {
	qts := StatsQueryTimeSeries{
		RuleKey:        tc.ruleKey,
		SharedRuleName: tc.sharedRuleName,
		ClusterName:    tc.clusterName,
		InstanceKeys:   tc.instanceKeys,
	}

	query := formatQuery([]string{tc.metric}, &qts)
	assert.Equal(t, query, tc.expectedQuery)
}

func TestFormatQuery(t *testing.T) {
	rk := api.RuleKey("r")
	sn := "s"
	cn := "c"
	ik1 := []string{"i1"}
	ik2 := []string{"i1", "i2"}
	m := "a-metric"
	testCases := []formatQueryTestCase{
		{m, nil, nil, nil, nil, `ts("a-metric")`},
		{m, nil, nil, &cn, nil, `ts("a-metric", upstream="c")`},
		{m, nil, nil, nil, ik1, `ts("a-metric", instance="i1")`},
		{m, nil, nil, nil, ik2, `ts("a-metric", instance="i1" or instance="i2")`},
		{m, nil, nil, &cn, ik1, `ts("a-metric", upstream="c" and instance="i1")`},
		{m, nil, nil, &cn, ik2, `ts("a-metric", upstream="c" and (instance="i1" or instance="i2"))`},

		{m, &rk, nil, nil, nil, `ts("a-metric", rule="r")`},
		{m, &rk, nil, &cn, nil, `ts("a-metric", rule="r" and upstream="c")`},
		{m, &rk, nil, nil, ik1, `ts("a-metric", rule="r" and instance="i1")`},
		{m, &rk, nil, nil, ik2, `ts("a-metric", rule="r" and (instance="i1" or instance="i2"))`},
		{m, &rk, nil, &cn, ik1, `ts("a-metric", rule="r" and upstream="c" and instance="i1")`},
		{m, &rk, nil, &cn, ik2, `ts("a-metric", rule="r" and upstream="c" and (instance="i1" or instance="i2"))`},

		{m, nil, &sn, nil, nil, `ts("a-metric", shared_rule="s")`},
		{m, nil, &sn, &cn, nil, `ts("a-metric", shared_rule="s" and upstream="c")`},
		{m, nil, &sn, nil, ik1, `ts("a-metric", shared_rule="s" and instance="i1")`},
		{m, nil, &sn, nil, ik2, `ts("a-metric", shared_rule="s" and (instance="i1" or instance="i2"))`},
		{m, nil, &sn, &cn, ik1, `ts("a-metric", shared_rule="s" and upstream="c" and instance="i1")`},
		{m, nil, &sn, &cn, ik2, `ts("a-metric", shared_rule="s" and upstream="c" and (instance="i1" or instance="i2"))`},

		{m, &rk, &sn, nil, nil, `ts("a-metric", rule="r" and shared_rule="s")`},
		{m, &rk, &sn, &cn, nil, `ts("a-metric", rule="r" and shared_rule="s" and upstream="c")`},
		{m, &rk, &sn, nil, ik1, `ts("a-metric", rule="r" and shared_rule="s" and instance="i1")`},
		{m, &rk, &sn, nil, ik2, `ts("a-metric", rule="r" and shared_rule="s" and (instance="i1" or instance="i2"))`},
		{m, &rk, &sn, &cn, ik1, `ts("a-metric", rule="r" and shared_rule="s" and upstream="c" and instance="i1")`},
		{m, &rk, &sn, &cn, ik2, `ts("a-metric", rule="r" and shared_rule="s" and upstream="c" and (instance="i1" or instance="i2"))`},
	}

	for _, tc := range testCases {
		tc.run(t)
	}
}

func TestNewWavefrontQueryBuilder(t *testing.T) {
	builder, err := newWavefrontQueryBuilder("not a url")
	assert.Nil(t, builder)
	assert.NonNil(t, err)

	builder, err = newWavefrontQueryBuilder(DefaultWavefrontServerUrl)
	assert.NonNil(t, builder)
	assert.Equal(t, builder.wavefrontServerUrl, DefaultWavefrontServerUrl)
	assert.Nil(t, err)

	builder, err = newWavefrontQueryBuilder("http://something.something.com")
	assert.NonNil(t, builder)
	assert.Equal(t, builder.wavefrontServerUrl, "http://something.something.com")
	assert.Nil(t, err)

	builder, err = newWavefrontQueryBuilder("http://something.com/with/path")
	assert.NonNil(t, builder)
	assert.Equal(t, builder.wavefrontServerUrl, "http://something.com/with/path")
	assert.Nil(t, err)

	// removes trailing slash
	builder, err = newWavefrontQueryBuilder("http://something.com/")
	assert.NonNil(t, builder)
	assert.Equal(t, builder.wavefrontServerUrl, "http://something.com")
	assert.Nil(t, err)
}

func TestWavefrontQueryBuilder(t *testing.T) {
	start := int64(1472667004)
	end := start + 3600
	orgKey := api.OrgKey("o")
	zoneName := "z"
	domainHost := "d"
	routeKey := api.RouteKey("r")
	method := "GET"

	qts := StatsQueryTimeSeries{
		QueryType:  Requests,
		DomainHost: &domainHost,
		RouteKey:   &routeKey,
		Method:     &method,
	}

	builder := wavefrontQueryBuilder{"https://wavefront.example.com"}

	u := builder.FormatWavefrontQueryUrl(
		start*1000000,
		end*1000000,
		Seconds,
		orgKey,
		zoneName,
		&qts,
	)
	url, err := url.Parse(u)
	assert.Nil(t, err)

	assert.Equal(t, url.Scheme, "https")
	assert.Equal(t, url.Host, "wavefront.example.com")
	assert.Equal(t, url.Path, "/chart/api")

	queryParams := url.Query()
	for k, v := range queryParams {
		if !assert.Equal(t, len(v), 1) {
			fmt.Println("multiple values for ", k)
		}
	}

	assert.Equal(t, queryParams.Get("g"), "s")
	assert.Equal(t, queryParams.Get("summarization"), "SUM")
	assert.Equal(t, queryParams.Get("strict"), "true")
	assert.Equal(t, queryParams.Get("s"), fmt.Sprintf("%d", start))
	assert.Equal(t, queryParams.Get("e"), fmt.Sprintf("%d", end))
	assert.Equal(
		t,
		queryParams.Get("q"),
		fmt.Sprintf(
			"default(0, rawsum(align(1s, sum, %s)))",
			formatQuery(
				[]string{
					formatMetric(
						orgKey,
						zoneName,
						&domainHost,
						&routeKey,
						&method,
						Requests,
					),
				},
				&qts,
			),
		),
	)
}

func TestWavefrontQueryBuilderLatencyP50(t *testing.T) {
	start := int64(1472667004)
	end := start + 3600
	orgKey := api.OrgKey("o")
	zoneName := "z"
	domainHost := "d"
	routeKey := api.RouteKey("r")
	method := "GET"

	qts := StatsQueryTimeSeries{
		QueryType:  LatencyP50,
		DomainHost: &domainHost,
		RouteKey:   &routeKey,
		Method:     &method,
	}

	builder := wavefrontQueryBuilder{"https://wavefront.example.com"}

	u := builder.FormatWavefrontQueryUrl(
		start*1000000,
		end*1000000,
		Seconds,
		orgKey,
		zoneName,
		&qts,
	)
	url, err := url.Parse(u)
	assert.Nil(t, err)

	assert.Equal(t, url.Scheme, "https")
	assert.Equal(t, url.Host, "wavefront.example.com")
	assert.Equal(t, url.Path, "/chart/api")

	queryParams := url.Query()
	for k, v := range queryParams {
		if !assert.Equal(t, len(v), 1) {
			fmt.Println("multiple values for ", k)
		}
	}

	assert.Equal(t, queryParams.Get("g"), "s")
	assert.Equal(t, queryParams.Get("summarization"), "MEAN")
	assert.Equal(t, queryParams.Get("strict"), "true")
	assert.Equal(t, queryParams.Get("s"), fmt.Sprintf("%d", start))
	assert.Equal(t, queryParams.Get("e"), fmt.Sprintf("%d", end))
	assert.Equal(
		t,
		queryParams.Get("q"),
		fmt.Sprintf(
			"default(0, percentile(50, align(1s, mean, %s)))",
			formatQuery(
				[]string{
					formatMetric(
						orgKey,
						zoneName,
						&domainHost,
						&routeKey,
						&method,
						LatencyP50,
					),
				},
				&qts,
			),
		),
	)
}

func TestWavefrontQueryBuilderSuccessRate(t *testing.T) {
	start := int64(1472667004)
	end := start + 3600
	orgKey := api.OrgKey("o")
	zoneName := "z"
	domainHost := "d"
	routeKey := api.RouteKey("r")
	method := "GET"

	qts := StatsQueryTimeSeries{
		QueryType:  SuccessRate,
		DomainHost: &domainHost,
		RouteKey:   &routeKey,
		Method:     &method,
	}

	builder := wavefrontQueryBuilder{"https://wavefront.example.com"}
	u := builder.FormatWavefrontQueryUrl(
		start*1000000,
		end*1000000,
		Seconds,
		orgKey,
		zoneName,
		&qts,
	)
	url, err := url.Parse(u)
	assert.Nil(t, err)

	assert.Equal(t, url.Scheme, "https")
	assert.Equal(t, url.Host, "wavefront.example.com")
	assert.Equal(t, url.Path, "/chart/api")

	queryParams := url.Query()
	for k, v := range queryParams {
		if !assert.Equal(t, len(v), 1) {
			fmt.Println("multiple values for ", k)
		}
	}

	ctxt := &queryContext{
		orgKey:      orgKey,
		zoneName:    zoneName,
		granularity: Seconds,
		qts:         &qts,
	}

	assert.Equal(t, queryParams.Get("g"), "s")
	assert.Equal(t, queryParams.Get("summarization"), "SUM")
	assert.Equal(t, queryParams.Get("strict"), "true")
	assert.Equal(t, queryParams.Get("s"), fmt.Sprintf("%d", start))
	assert.Equal(t, queryParams.Get("e"), fmt.Sprintf("%d", end))
	assert.Equal(t, queryParams.Get("q"), queryExprMap[SuccessRate].Query(ctxt))
}

func TestWavefrontQueryUrlGranularities(t *testing.T) {
	start := int64(1472667004)
	end := start + 3600
	orgKey := api.OrgKey("o")
	zoneName := "z"
	domainHost := "d"
	routeKey := api.RouteKey("r")
	method := "GET"

	qts := StatsQueryTimeSeries{
		QueryType:  SuccessRate,
		DomainHost: &domainHost,
		RouteKey:   &routeKey,
		Method:     &method,
	}

	forEachTimeGranularity(func(tg TimeGranularity) {
		firstLetter := strings.ToLower(tg.String()[0:1])

		builder := wavefrontQueryBuilder{"https://wavefront.example.com"}
		u := builder.FormatWavefrontQueryUrl(
			start*1000000,
			end*1000000,
			tg,
			orgKey,
			zoneName,
			&qts,
		)
		url, err := url.Parse(u)
		assert.Nil(t, err)

		queryParams := url.Query()
		assert.Equal(t, queryParams.Get("g"), firstLetter)
	})
}

func TestEscape(t *testing.T) {
	testcases := [][]string{
		{"simple", "simple"},
		{"0123456789", "0123456789"},
		{"SIMPLE", "SIMPLE"},
		{"this-is_ok_too", "this-is_ok_too"},
		{"a/b+c:d.e!", "a_b_c_d_e_"},
	}

	for _, tc := range testcases {
		input := tc[0]
		assert.Group(fmt.Sprintf("input: `%s`", input), t, func(g *assert.G) {
			expected := tc[1]
			assert.Equal(g, escape(input), expected)
		})
	}
}

type queryExprTestCase struct {
	queryType     QueryType
	expectedQuery string
}

func (tc queryExprTestCase) run(t *testing.T, ctxt *queryContext) {
	ctxt.qts.QueryType = tc.queryType
	expr := queryExprMap[ctxt.qts.QueryType]
	query := expr.Query(ctxt)
	assert.Equal(
		t,
		query,
		fmt.Sprintf("default(0, %s)", tc.expectedQuery),
	)
}

func TestQueryExprs(t *testing.T) {
	ok := api.OrgKey("o")
	zn := "z"

	successfulResponsesQuery :=
		`rawsum(align(1s, sum, ts("o.z.*.*.*.responses.1*" or "o.z.*.*.*.responses.2*" or "o.z.*.*.*.responses.3*")))`
	testCases := []queryExprTestCase{
		{Requests, `rawsum(align(1s, sum, ts("o.z.*.*.*.requests")))`},
		{Responses, `rawsum(align(1s, sum, ts("o.z.*.*.*.responses.*")))`},
		{SuccessfulResponses, successfulResponsesQuery},
		{ErrorResponses, `rawsum(align(1s, sum, ts("o.z.*.*.*.responses.4*")))`},
		{FailureResponses, `rawsum(align(1s, sum, ts("o.z.*.*.*.responses.5*")))`},
		{LatencyP50, `percentile(50, align(1s, mean, ts("o.z.*.*.*.latency")))`},
		{LatencyP99, `percentile(99, align(1s, mean, ts("o.z.*.*.*.latency")))`},
		{SuccessRate, `(` + successfulResponsesQuery + `/rawsum(align(1s, sum, ts("o.z.*.*.*.requests"))))`},
	}

	for _, tc := range testCases {
		tc.run(t, &queryContext{ok, zn, Seconds, &StatsQueryTimeSeries{}})
	}
}

func TestQueryExprWithTags(t *testing.T) {
	ok := api.OrgKey("o")
	zn := "z"
	cn := "c"
	qts := StatsQueryTimeSeries{
		ClusterName:  &cn,
		InstanceKeys: []string{"i1"},
	}
	queryExprTestCase{
		Requests,
		`rawsum(align(1s, sum, ts("o.z.*.*.*.requests", upstream="c" and instance="i1")))`,
	}.run(t, &queryContext{ok, zn, Seconds, &qts})
}

const wavefrontResponse = `{
  "query": "default(0, rawsum(ts(stats.counters.api.requests.count)))",
  "name": "default(0, rawsum(ts(stats.counters.api.requests.count)))",
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
