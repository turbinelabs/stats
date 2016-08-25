package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/turbinelabs/test/assert"
	tbntime "github.com/turbinelabs/time"
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

	return &http.Request{URL: u}
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

func testZoneKeyValidation(t *testing.T, useHumaneEncoding bool) {
	req := mkRequest(t, queryMap{}, useHumaneEncoding)
	rw := httptest.NewRecorder()
	handler := (&queryHandler{}).AsHandler()

	handler(rw, req)

	assert.Equal(t, rw.Code, 400)
	assert.MatchesRegex(t, rw.Body.String(), "query requires zone_key")
}

func TestRunQueryZoneKeyValidation(t *testing.T) {
	testZoneKeyValidation(t, true)
	testZoneKeyValidation(t, false)
}

func testQueryTypeEmpty(t *testing.T, useHumaneEncoding bool) {
	params := queryMap{
		"zone_key": "zk",
		"timeseries": []queryMap{
			{"domain_key": "dk"},
		},
	}

	req := mkRequest(t, params, useHumaneEncoding)

	rw := httptest.NewRecorder()
	handler := (&queryHandler{}).AsHandler()

	handler(rw, req)

	assert.Equal(t, rw.Code, 400)
	assert.MatchesRegex(t, rw.Body.String(), "query contains invalid query type")
}

func testQueryTypeInvalid(t *testing.T, useHumaneEncoding bool) {
	params := queryMap{
		"zone_key": "zk",
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

func TestNormalizeTimeRangeDefault(t *testing.T) {
	before := tbntime.ToUnixMicro(time.Now().Truncate(time.Second))
	start, end, err := normalizeTimeRange(StatsTimeRange{})
	after := tbntime.ToUnixMicro(time.Now())

	assert.True(t, before <= end && end <= after)
	assert.Equal(t, start, end-3600000000)
	assert.Nil(t, err)
}

func TestNormalizeTimeRangeErrors(t *testing.T) {
	when := tbntime.ToUnixMicro(time.Now())
	start, end, err := normalizeTimeRange(StatsTimeRange{End: &when})
	assert.Equal(t, start, int64(0))
	assert.Equal(t, end, int64(0))
	assert.ErrorContains(t, err, "time range start is not set")

	duration := int64(3600000000)
	start, end, err = normalizeTimeRange(StatsTimeRange{Duration: &duration})
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
