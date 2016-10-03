package handler

import (
	"encoding/json"
	"fmt"
)

type QueryType int

const (
	UnknownQueryType QueryType = iota
	Requests
	Responses
	SuccessfulResponses
	ErrorResponses
	FailureResponses
	LatencyP50
	LatencyP99
	SuccessRate
)

var _dummy = QueryType(0)
var _ json.Marshaler = &_dummy
var _ json.Unmarshaler = &_dummy

const (
	unknown             = "unknown"
	requests            = "requests"
	responses           = "responses"
	successfulResponses = "success"
	errorResponses      = "error"
	failureResponses    = "failure"
	latency_p50         = "latency_p50"
	latency_p99         = "latency_p99"
	successRate         = "success_rate"
)

var queryTypeNames = [...]string{
	unknown,
	requests,
	responses,
	successfulResponses,
	errorResponses,
	failureResponses,
	latency_p50,
	latency_p99,
	successRate,
}

var maxQueryType = QueryType(len(queryTypeNames) - 1)

func IsValidQueryType(i QueryType) bool {
	return i >= 1 && i <= maxQueryType
}

func QueryTypeFromName(s string) QueryType {
	for idx, name := range queryTypeNames {
		if idx == 0 {
			continue
		}
		if name == s {
			return QueryType(idx)
		}
	}

	return UnknownQueryType
}

func (i QueryType) String() string {
	if !IsValidQueryType(i) {
		return fmt.Sprintf("unknown(%d)", i)
	}
	return queryTypeNames[i]
}

func (i *QueryType) MarshalJSON() ([]byte, error) {
	if i == nil {
		return nil, fmt.Errorf("cannot marshal unknown query type (nil)")
	}

	qt := *i
	if !IsValidQueryType(qt) {
		return nil, fmt.Errorf("cannot marshal unknown query type (%d)", qt)
	}

	name := queryTypeNames[qt]
	b := make([]byte, 0, len(name)+2)
	b = append(b, '"')
	b = append(b, name...)
	return append(b, '"'), nil
}

func (i *QueryType) UnmarshalJSON(bytes []byte) error {
	if i == nil {
		return fmt.Errorf("cannot unmarshal into nil QueryType")
	}

	length := len(bytes)
	if length <= 2 || bytes[0] != '"' || bytes[length-1] != '"' {
		return fmt.Errorf("cannot unmarshal invalid JSON: `%s`", string(bytes))
	}

	unmarshalName := string(bytes[1 : length-1])

	qt := QueryTypeFromName(unmarshalName)
	if qt == UnknownQueryType {
		return fmt.Errorf("cannot unmarshal unknown query type `%s`", unmarshalName)
	}

	*i = qt
	return nil
}

func (i *QueryType) UnmarshalForm(value string) error {
	if i == nil {
		return fmt.Errorf("cannot unmarshal into nil QueryType")
	}

	qt := QueryTypeFromName(value)
	if qt == UnknownQueryType {
		return fmt.Errorf("cannot unmarshal unknown query type `%s`", value)
	}

	*i = qt
	return nil
}
