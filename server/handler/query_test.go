package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/turbinelabs/api"
	"github.com/turbinelabs/stats/server/handler/requestcontext"
	tbntime "github.com/turbinelabs/stdlib/time"
	"github.com/turbinelabs/test/assert"
)

// Nested map structure for testing unmarshaling into API objects. Can
// be be directly marshaled into JSON or "collapsed" into
// go-playground/form-style query parameters.
type queryMap map[string]interface{}

// Collapse nested map into single-level map with go-playground/form keys.
func (q queryMap) collapse() queryMap {
	newq := queryMap{}
	for k, v := range q {
		if subq, ok := v.(queryMap); ok {
			subq = subq.collapse()

			for k2, v2 := range subq {
				newq[k+"."+k2] = v2
			}
		} else if subqa, ok := v.([]queryMap); ok {
			for idx, v2 := range subqa {
				subq := v2.collapse()
				newk := fmt.Sprintf("%s[%d]", k, idx)

				for k3, v3 := range subq {
					newq[newk+"."+k3] = v3
				}
			}
		} else if suba, ok := v.([]interface{}); ok {
			for idx, v2 := range suba {
				newk := fmt.Sprintf("%s[%d]", k, idx)
				newq[newk] = v2
			}
		} else {
			newq[k] = v
		}
	}

	return newq
}

func mkQueryParams(t *testing.T, query queryMap, useHumaneEncoding bool) string {
	params := make([]string, 0, len(query))

	if useHumaneEncoding {
		query = queryMap{"query": query}.collapse()
		for name, value := range query {
			var valueStr string
			if m, ok := value.(json.Marshaler); ok {
				b, err := m.MarshalJSON()
				if err != nil {
					t.Fatalf("test error marshaling query param: %v", err)
				}
				valueStr = string(b)
			} else {
				valueStr = fmt.Sprintf("%s", value)
			}

			params = append(params, fmt.Sprintf("%s=%s", name, valueStr))
		}
	} else {
		b, err := json.Marshal(query)
		if err != nil {
			t.Fatalf("test error marshaling json query param: %v", err)
		}

		params = append(params, fmt.Sprintf("query=%s", string(b)))
	}

	return "?" + strings.Join(params, "&")
}

func mkRequest(
	t *testing.T,
	query queryMap,
	useHumaneEncoding bool,
) *http.Request {
	params := mkQueryParams(t, query, useHumaneEncoding)
	u, err := url.Parse("http://foo.com" + params)
	if err != nil {
		t.Fatalf("Failure to construct test object: %v", err)
	}

	req := &http.Request{URL: u}
	reqCtxt := requestcontext.New(req)
	reqCtxt.SetOrgKey(api.OrgKey("1234"))
	return req
}

func TestNewQueryHandler(t *testing.T) {
	wavefrontApiToken := "api-token"
	qh, err := NewQueryHandler(DefaultWavefrontServerUrl, wavefrontApiToken, true)
	assert.Nil(t, err)

	qhImpl := qh.(*queryHandler)
	assert.Equal(t, qhImpl.wavefrontApiToken, wavefrontApiToken)
	assert.NonNil(t, qhImpl.client)
	assert.NonNil(t, qhImpl.formatQueryUrl)
	assert.True(t, qhImpl.verboseLogging)
}

func testHandlerDecodingError(t *testing.T, useHumaneEncoding bool) {
	var params string
	if useHumaneEncoding {
		params = "?query.timeseries[0].query_type=nope nope"
	} else {
		params = `?query={not json}`
	}
	u, err := url.Parse("http://foo.com" + params)
	if err != nil {
		t.Fatalf("Failure to construct test object: %v", err)
	}

	req := &http.Request{URL: u}
	reqCtxt := requestcontext.New(req)
	reqCtxt.SetOrgKey(api.OrgKey("1234"))

	rw := httptest.NewRecorder()
	handler := (&queryHandler{}).AsHandler()

	handler(rw, req)

	assert.Equal(t, rw.Code, 400)
	assert.MatchesRegex(t, rw.Body.String(), "invalid query argument; unable to process")
}

func TestHandlerDecodingError(t *testing.T) {
	testHandlerDecodingError(t, true)
	testHandlerDecodingError(t, false)
}

func TestHandlerMissingOrgKey(t *testing.T) {
	u, err := url.Parse("http://foo.com")
	if err != nil {
		t.Fatalf("Failure to construct test object: %v", err)
	}

	req := &http.Request{URL: u}

	rw := httptest.NewRecorder()
	handler := (&queryHandler{}).AsHandler()

	handler(rw, req)

	assert.Equal(t, rw.Code, 500)
	assert.MatchesRegex(t, rw.Body.String(), "authorization config error")

}

func testZoneNameValidation(t *testing.T, useHumaneEncoding bool) {
	req := mkRequest(t, queryMap{}, useHumaneEncoding)
	rw := httptest.NewRecorder()
	handler := (&queryHandler{}).AsHandler()

	handler(rw, req)

	assert.Equal(t, rw.Code, 400)
	assert.MatchesRegex(t, rw.Body.String(), "query requires zone_name")
}

func TestRunQueryZoneNameValidation(t *testing.T) {
	testZoneNameValidation(t, true)
	testZoneNameValidation(t, false)
}

func testQueryTypeEmpty(t *testing.T, useHumaneEncoding bool) {
	handler := (&queryHandler{}).AsHandler()

	params := queryMap{
		"zone_name": "zn",
		"timeseries": []queryMap{
			{"domain_host": "dh"},
		},
	}

	req := mkRequest(t, params, useHumaneEncoding)
	rw := httptest.NewRecorder()
	handler(rw, req)

	assert.Equal(t, rw.Code, 400)
	assert.MatchesRegex(t, rw.Body.String(), `query\[0\] contains invalid query type`)

	params = queryMap{
		"zone_name": "zn",
		"timeseries": []queryMap{
			{
				"name":        "this query",
				"domain_host": "dh",
			},
		},
	}

	req = mkRequest(t, params, useHumaneEncoding)
	rw = httptest.NewRecorder()
	handler(rw, req)

	assert.Equal(t, rw.Code, 400)
	assert.MatchesRegex(t, rw.Body.String(), "query 'this query' contains invalid query type")
}

func testQueryTypeInvalid(t *testing.T, useHumaneEncoding bool) {
	params := queryMap{
		"zone_name": "zn",
		"timeseries": []queryMap{
			{"query_type": "invalid"},
		},
	}

	req := mkRequest(t, params, useHumaneEncoding)

	rw := httptest.NewRecorder()
	handler := (&queryHandler{}).AsHandler()

	handler(rw, req)

	assert.Equal(t, rw.Code, 400)
	assert.MatchesRegex(t, rw.Body.String(), "invalid query argument")
}

func TestRunQueryQueryTypeValidation(t *testing.T) {
	testQueryTypeEmpty(t, true)
	testQueryTypeEmpty(t, false)

	testQueryTypeInvalid(t, true)
	testQueryTypeInvalid(t, false)
}

func testRuleKeyValidation(
	t *testing.T,
	routeKey *api.RouteKey,
	sharedRuleName *string,
	valid bool,
) {
	ruleKey := api.RuleKey("some-rule-key")
	query := &StatsQuery{
		ZoneName: "zone_name",
		TimeSeries: []StatsQueryTimeSeries{
			{
				QueryType:      Requests,
				RouteKey:       routeKey,
				SharedRuleName: sharedRuleName,
				RuleKey:        &ruleKey,
			},
		},
	}

	err := validateQuery(query)
	if valid {
		assert.Nil(t, err)
	} else {
		assert.NonNil(t, err)
		assert.ErrorContains(
			t,
			err,
			"query[0] must have a RouteKey and/or SharedRuleName to scope the given RuleKey",
		)
	}

	query = &StatsQuery{
		ZoneName: "zone_name",
		TimeSeries: []StatsQueryTimeSeries{
			{
				Name:           "this query",
				QueryType:      Requests,
				RouteKey:       routeKey,
				SharedRuleName: sharedRuleName,
				RuleKey:        &ruleKey,
			},
		},
	}

	err = validateQuery(query)
	if valid {
		assert.Nil(t, err)
	} else {
		assert.NonNil(t, err)
		assert.ErrorContains(
			t,
			err,
			"query 'this query' must have a RouteKey and/or SharedRuleName to scope the given RuleKey",
		)
	}
}

func TestRunQueryRuleKeyValidation(t *testing.T) {
	rk := api.RouteKey("rk")
	sr := "sr"

	testRuleKeyValidation(t, nil, nil, false)
	testRuleKeyValidation(t, &rk, nil, true)
	testRuleKeyValidation(t, nil, &sr, true)
	testRuleKeyValidation(t, &rk, &sr, true)
}

func testRunQuery(t *testing.T, useHumaneEncoding bool) {
	apiToken := "the-api-token"

	mockWavefrontHandler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.Header.Get("X-Auth-Token"), apiToken)

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(wavefrontResponse))
		},
	)
	server := httptest.NewServer(mockWavefrontHandler)
	defer server.Close()

	formatTestQueryUrl :=
		func(
			start, end int64,
			granularity TimeGranularity,
			orgKey api.OrgKey,
			zoneName string,
			qts *StatsQueryTimeSeries,
		) string {
			return server.URL
		}

	queryHandler := &queryHandler{
		wavefrontApiToken: apiToken,
		client:            http.DefaultClient,
		formatQueryUrl:    formatTestQueryUrl,
	}

	params := queryMap{
		"zone_name": "zn",
		"timeseries": []queryMap{
			{"query_type": "requests"},
		},
	}

	req := mkRequest(t, params, useHumaneEncoding)

	rw := httptest.NewRecorder()
	handler := queryHandler.AsHandler()

	handler(rw, req)

	assert.Equal(t, rw.Code, 200)

	err := json.Unmarshal(rw.Body.Bytes(), &StatsQueryResult{})
	assert.Nil(t, err)
}

func TestRunQuery(t *testing.T) {
	testRunQuery(t, true)
	testRunQuery(t, false)
}

type assertTimeWithinBoundsFunc func(t *testing.T, tm time.Time)

func boundedTest(truncateTo time.Duration, f func(assertTimeWithinBoundsFunc)) {
	before := time.Now().Truncate(truncateTo).UnixNano()

	withinBoundsFunc := func(t *testing.T, tm time.Time) {
		after := time.Now().UnixNano()
		assert.True(t, before <= tm.UnixNano())
		assert.True(t, tm.UnixNano() <= after)
	}

	f(withinBoundsFunc)
}

func TestNormalizeTimeRangeDefault(t *testing.T) {
	boundedTest(time.Second, func(assertTimeWithinBounds assertTimeWithinBoundsFunc) {
		start, end, err := normalizeTimeRange(StatsTimeRange{})

		assertTimeWithinBounds(t, tbntime.FromUnixMicro(end))
		assert.Equal(t, start, end-3600000000)
		assert.Nil(t, err)
	})
}

func TestNormalizeTimeRangeErrors(t *testing.T) {
	when := tbntime.ToUnixMicro(time.Now())
	start, end, err := normalizeTimeRange(StatsTimeRange{End: &when})
	assert.Equal(t, start, int64(0))
	assert.Equal(t, end, int64(0))
	assert.ErrorContains(t, err, "time range start is not set")

	start, end, err = normalizeTimeRange(StatsTimeRange{Start: &when, End: &when})
	assert.Equal(t, start, int64(0))
	assert.Equal(t, end, int64(0))
	assert.ErrorContains(t, err, "empty time range: start equals end")

	zeroDuration := int64(0)
	start, end, err = normalizeTimeRange(StatsTimeRange{Start: &when, Duration: &zeroDuration})
	assert.Equal(t, start, int64(0))
	assert.Equal(t, end, int64(0))
	assert.ErrorContains(t, err, "empty time range: duration is zero")

	start, end, err = normalizeTimeRange(StatsTimeRange{Start: &when})
	assert.Equal(t, start, int64(0))
	assert.Equal(t, end, int64(0))
	assert.ErrorContains(t, err, "time range start is set, but not end or duration")
}

func TestNormalizeTimeRangeStartEnd(t *testing.T) {
	end := tbntime.ToUnixMicro(time.Now())
	start := end - 180000000

	normalizedStart, normalizedEnd, err := normalizeTimeRange(
		StatsTimeRange{Start: &start, End: &end},
	)

	assert.Equal(t, normalizedStart, start)
	assert.Equal(t, normalizedEnd, end)
	assert.Nil(t, err)

	// reversed start/end
	normalizedStart, normalizedEnd, err = normalizeTimeRange(
		StatsTimeRange{Start: &end, End: &start},
	)

	assert.Equal(t, normalizedStart, start)
	assert.Equal(t, normalizedEnd, end)
	assert.Nil(t, err)
}

func TestNormalizeTimeRangeDuration(t *testing.T) {
	boundedTest(time.Second, func(assertTimeWithinBounds assertTimeWithinBoundsFunc) {
		duration := int64(7200000000)
		start, end, err := normalizeTimeRange(StatsTimeRange{Duration: &duration})

		assertTimeWithinBounds(t, tbntime.FromUnixMicro(end))
		assert.Equal(t, start, end-7200000000)
		assert.Nil(t, err)
	})
}

func TestNormalizeTimeRangeStartDuration(t *testing.T) {
	duration := int64(180000000)
	end := tbntime.ToUnixMicro(time.Now())
	start := end - duration

	normalizedStart, normalizedEnd, err := normalizeTimeRange(
		StatsTimeRange{Start: &start, Duration: &duration},
	)

	assert.Equal(t, normalizedStart, start)
	assert.Equal(t, normalizedEnd, end)
	assert.Nil(t, err)
}

func TestRunQueries(t *testing.T) {
	apiToken := "the-api-token"

	queryHandler := &queryHandler{
		wavefrontApiToken: apiToken,
		client:            http.DefaultClient,
	}

	mockWavefrontHandler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.Header.Get("X-Auth-Token"), apiToken)

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(wavefrontResponse))
		},
	)
	server := httptest.NewServer(mockWavefrontHandler)
	defer server.Close()

	urls := []string{
		server.URL + "?q=1",
		server.URL + "?q=2",
	}

	result, err := queryHandler.runQueries(urls)
	assert.Nil(t, err)
	assert.Equal(t, len(result), len(urls))
	for _, ts := range result {
		assert.Equal(t, len(ts.Points), 25)
	}
}

func TestRunQueriesWith500s(t *testing.T) {
	apiToken := "the-api-token"

	queryHandler := &queryHandler{
		wavefrontApiToken: apiToken,
		client:            http.DefaultClient,
	}

	mockWavefrontHandler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.Header.Get("X-Auth-Token"), apiToken)

			if r.URL.Query().Get("q") == "2" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(wavefrontResponse))
		},
	)
	server := httptest.NewServer(mockWavefrontHandler)
	defer server.Close()

	urls := []string{
		server.URL + "?q=1",
		server.URL + "?q=2",
	}

	result, err := queryHandler.runQueries(urls)
	assert.ErrorContains(t, err, "Error 500 querying wavefront")
	assert.Equal(t, len(result), 0)
}

func TestRunQueriesWithInvalidJson(t *testing.T) {
	apiToken := "the-api-token"

	queryHandler := &queryHandler{
		wavefrontApiToken: apiToken,
		client:            http.DefaultClient,
	}

	mockWavefrontHandler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.Header.Get("X-Auth-Token"), apiToken)

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(wavefrontResponse))
			if r.URL.Query().Get("q") == "2" {
				w.Write([]byte("garbage"))
			}

		},
	)
	server := httptest.NewServer(mockWavefrontHandler)
	defer server.Close()

	urls := []string{
		server.URL + "?q=1",
		server.URL + "?q=2",
	}

	result, err := queryHandler.runQueries(urls)
	assert.ErrorContains(t, err, "unexpected data beyond query response")
	assert.Equal(t, len(result), 0)
}

func TestRunQueriesWithRequestError(t *testing.T) {
	apiToken := "the-api-token"

	queryHandler := &queryHandler{
		wavefrontApiToken: apiToken,
		client:            http.DefaultClient,
	}

	mockWavefrontHandler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(wavefrontResponse))
		},
	)
	server := httptest.NewServer(mockWavefrontHandler)
	defer server.Close()

	urls := []string{
		server.URL + "?q=1",
		server.URL + "99999?q=2",
	}

	result, err := queryHandler.runQueries(urls)
	assert.ErrorContains(t, err, "invalid port")
	assert.Equal(t, len(result), 0)
}

func TestMakeQueryResult(t *testing.T) {
	duration := int64(3600 * 1e6)
	end := tbntime.ToUnixMicro(time.Now().Truncate(time.Second))
	start := end - duration

	queries := []StatsQueryTimeSeries{
		{Name: "this one", QueryType: Responses},
		{Name: "that one", QueryType: Requests},
	}

	results := []StatsTimeSeries{
		{Points: []StatsPoint{{Value: 1.0, Timestamp: start}}},
		{Points: []StatsPoint{{Value: 2.0, Timestamp: start}}},
	}

	r, err := makeQueryResult(start, end, Minutes, queries, results)

	assert.Nil(t, err)

	assert.Equal(t, *r.TimeRange.Start, start)
	assert.Equal(t, *r.TimeRange.End, end)
	assert.Equal(t, *r.TimeRange.Duration, duration)
	assert.Equal(t, r.TimeRange.Granularity, Minutes)

	assert.Equal(t, len(r.TimeSeries), len(queries))
	assert.DeepEqual(t, r.TimeSeries[0].Query, queries[0])
	assert.DeepEqual(t, r.TimeSeries[1].Query, queries[1])

}

func TestMakeQueryResultMismatchedInput(t *testing.T) {
	r, err := makeQueryResult(
		0,
		0,
		Hours,
		[]StatsQueryTimeSeries{{Name: "name"}},
		[]StatsTimeSeries{},
	)

	assert.Nil(t, r)
	assert.NonNil(t, err)
}

func testJsonAndFormTagsMatch(t testing.TB, typeRef reflect.Type) {
	assert.Group(typeRef.Name(), t, func(g *assert.G) {
		if !assert.True(g, typeRef.Kind() == reflect.Struct) {
			return
		}

		for i := 0; i < typeRef.NumField(); i++ {
			f := typeRef.Field(i)

			jsonTag := f.Tag.Get("json")
			jsonTagParts := strings.SplitN(jsonTag, ",", 2)
			jsonTag = jsonTagParts[0]

			formTag := f.Tag.Get("form")

			assert.Equal(g, formTag, jsonTag)

			switch f.Type.Kind() {
			case reflect.Struct:
				testJsonAndFormTagsMatch(g, f.Type)

			case reflect.Ptr, reflect.Array, reflect.Slice, reflect.Map:
				if f.Type.Elem().Kind() == reflect.Struct {
					testJsonAndFormTagsMatch(g, f.Type.Elem())
				}
			}
		}
	})
}

func TestJsonAndFormTagsMatch(t *testing.T) {
	testJsonAndFormTagsMatch(t, reflect.TypeOf(StatsQuery{}))
}
