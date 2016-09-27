package handler

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/turbinelabs/test/assert"
)

func forEachQueryType(f func(QueryType)) {
	for i := 1; i <= int(maxQueryType); i++ {
		qt := QueryType(i)
		f(qt)
	}
}

type testStruct struct {
	QueryType QueryType `json:"query_type"`
}

func TestQueryTypeString(t *testing.T) {
	assert.Equal(t, Requests.String(), "requests")
	assert.Equal(t, UnknownQueryType.String(), "unknown(0)")
	assert.Equal(t, QueryType(100).String(), "unknown(100)")
}

func TestIsValidQueryType(t *testing.T) {
	invalid := []QueryType{
		QueryType(-1),
		QueryType(0),
		QueryType(maxQueryType + 1),
	}

	for _, qt := range invalid {
		assert.False(t, IsValidQueryType(qt))
	}

	forEachQueryType(func(qt QueryType) {
		assert.True(t, IsValidQueryType(qt))
	})
}

func TestQueryTypeFromName(t *testing.T) {
	validQueryTypes := map[QueryType]string{
		Requests:            requests,
		Responses:           responses,
		SuccessfulResponses: successfulResponses,
		ErrorResponses:      errorResponses,
		FailureResponses:    failureResponses,
		LatencyP50:          latency_p50,
		LatencyP99:          latency_p99,
	}

	for expectedQt, name := range validQueryTypes {
		qt := QueryTypeFromName(name)
		assert.Equal(t, qt, expectedQt)
	}

	invalidQueryTypes := []string{"bob", "unknown", "1"}

	for _, name := range invalidQueryTypes {
		qt := QueryTypeFromName(name)
		assert.Equal(t, qt, UnknownQueryType)
	}
}

func TestQueryTypeMarshalJSON(t *testing.T) {
	queryTypes := map[QueryType]string{
		Requests:            requests,
		Responses:           responses,
		SuccessfulResponses: successfulResponses,
		ErrorResponses:      errorResponses,
		FailureResponses:    failureResponses,
		LatencyP50:          latency_p50,
		LatencyP99:          latency_p99,
	}

	for queryType, name := range queryTypes {
		bytes, err := queryType.MarshalJSON()
		assert.Nil(t, err)
		expected := []byte(fmt.Sprintf(`"%s"`, name))
		assert.DeepEqual(t, bytes, expected)
	}
}

func TestQueryTypeMarshalJSONUnknown(t *testing.T) {
	unknownQueryTypes := []QueryType{UnknownQueryType, QueryType(maxQueryType + 1)}

	for _, unknownQueryType := range unknownQueryTypes {
		bytes, err := unknownQueryType.MarshalJSON()
		assert.Nil(t, bytes)
		assert.ErrorContains(t, err, "cannot marshal unknown query type")
	}
}

func TestQueryTypeMarshalJSONNil(t *testing.T) {
	var queryType *QueryType

	bytes, err := queryType.MarshalJSON()
	assert.ErrorContains(t, err, "cannot marshal unknown query type (nil)")
	assert.Nil(t, bytes)
}

func TestQueryTypeUnmarshalJSON(t *testing.T) {
	quoted := func(s string) string {
		return fmt.Sprintf(`"%s"`, s)
	}

	queryTypes := map[string]QueryType{
		quoted(requests):            Requests,
		quoted(responses):           Responses,
		quoted(successfulResponses): SuccessfulResponses,
		quoted(errorResponses):      ErrorResponses,
		quoted(failureResponses):    FailureResponses,
		quoted(latency_p50):         LatencyP50,
		quoted(latency_p99):         LatencyP99,
	}

	for data, expectedQueryType := range queryTypes {
		var queryType QueryType

		err := queryType.UnmarshalJSON([]byte(data))
		assert.Nil(t, err)
		assert.Equal(t, queryType, expectedQueryType)
	}
}

func TestQueryTypeUnmarshalJSONUnknown(t *testing.T) {
	unknownQueryTypes := []string{`"unknown"`, `"nope"`}

	for _, unknownName := range unknownQueryTypes {
		var queryType QueryType

		err := queryType.UnmarshalJSON([]byte(unknownName))
		assert.ErrorContains(t, err, "cannot unmarshal unknown query type")
	}
}

func TestQueryTypeUnmarshalJSONNil(t *testing.T) {
	var queryType *QueryType

	err := queryType.UnmarshalJSON([]byte(`"requests"`))
	assert.ErrorContains(t, err, "cannot unmarshal into nil QueryType")
}

func TestQueryTypeUnmarshalJSONInvalid(t *testing.T) {
	invalidQueryTypes := []string{``, `"`, `x`, `xx`, `"x`, `x"`, `'something'`}

	for _, invalidName := range invalidQueryTypes {
		var queryType QueryType

		err := queryType.UnmarshalJSON([]byte(invalidName))
		assert.ErrorContains(t, err, "cannot unmarshal invalid JSON")
	}
}

func TestQueryTypeUnmarshalForm(t *testing.T) {
	queryTypes := map[string]QueryType{
		requests:            Requests,
		responses:           Responses,
		successfulResponses: SuccessfulResponses,
		errorResponses:      ErrorResponses,
		failureResponses:    FailureResponses,
		latency_p50:         LatencyP50,
		latency_p99:         LatencyP99,
	}

	for data, expectedQueryType := range queryTypes {
		var queryType QueryType

		err := queryType.UnmarshalForm(data)
		assert.Nil(t, err)
		assert.Equal(t, queryType, expectedQueryType)
	}
}

func TestQueryTypeUnmarshalFormUnknown(t *testing.T) {
	unknownQueryTypes := []string{`unknown`, `nope`}

	for _, unknownName := range unknownQueryTypes {
		var queryType QueryType

		err := queryType.UnmarshalForm(unknownName)
		assert.ErrorContains(t, err, "cannot unmarshal unknown query type")
	}
}

func TestQueryTypeUnmarshalFormNil(t *testing.T) {
	var queryType *QueryType

	err := queryType.UnmarshalForm(`requests`)
	assert.ErrorContains(t, err, "cannot unmarshal into nil QueryType")
}

func TestQueryTypeRoundTripStruct(t *testing.T) {
	expected := testStruct{QueryType: Responses}

	bytes, err := json.Marshal(&expected)
	assert.Nil(t, err)
	assert.NonNil(t, bytes)
	assert.Equal(t, string(bytes), `{"query_type":"responses"}`)

	var ts testStruct
	err = json.Unmarshal(bytes, &ts)
	assert.Nil(t, err)
	assert.Equal(t, ts, expected)
}
