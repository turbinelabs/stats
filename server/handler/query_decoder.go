package handler

import (
	"errors"

	"github.com/turbinelabs/api/service/stats/querytype"
	"github.com/turbinelabs/api/service/stats/timegranularity"
	"github.com/turbinelabs/server/handler"
)

var noQueryTypeDecodeError = errors.New("Could not decode query_type, no value specified.")
var noTimeGranularityDecodeError = errors.New("Could not decode granularity, no value specified.")

var QueryDecoder handler.QueryDecoder

func init() {
	QueryDecoder = handler.NewQueryDecoder("form", "query", "query")
	QueryDecoder.RegisterCustomTypeFunc(unmarshalQueryTypeFromForm, querytype.Unknown)
	QueryDecoder.RegisterCustomTypeFunc(unmarshalTimeGranularityFromForm, timegranularity.Unknown)
}

func unmarshalQueryTypeFromForm(vals []string) (interface{}, error) {
	if len(vals) == 0 || vals[0] == "" {
		return nil, noQueryTypeDecodeError
	}

	var qt querytype.QueryType

	err := qt.UnmarshalForm(vals[0])
	if err != nil {
		return nil, err
	}

	return qt, err
}

func unmarshalTimeGranularityFromForm(vals []string) (interface{}, error) {
	if len(vals) == 0 || vals[0] == "" {
		return nil, noTimeGranularityDecodeError
	}

	var tg timegranularity.TimeGranularity

	err := tg.UnmarshalForm(vals[0])
	if err != nil {
		return nil, err
	}

	return tg, err
}
