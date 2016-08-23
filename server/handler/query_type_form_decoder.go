package handler

import (
	"errors"

	"github.com/turbinelabs/server/handler"
)

var noQueryTypeDecodeError = errors.New("Could not decode query_type, no value specified.")

func init() {
	handler.StructDecoder.RegisterCustomTypeFunc(unmarshalQueryTypeFromForm, UnknownQueryType)
}

func unmarshalQueryTypeFromForm(vals []string) (interface{}, error) {
	if len(vals) == 0 || vals[0] == "" {
		return nil, noQueryTypeDecodeError
	}

	var qt QueryType

	err := qt.UnmarshalForm(vals[0])
	if err != nil {
		return nil, err
	}

	return qt, err
}
