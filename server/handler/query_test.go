package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

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
