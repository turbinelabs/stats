package handler

import (
	"encoding/json"
	"fmt"
)

type TimeGranularity int

const (
	Seconds TimeGranularity = iota
	Minutes
	Hours
	UnknownTimeGranularity
)

var _dummyGranularity = Seconds
var _ json.Marshaler = &_dummyGranularity
var _ json.Unmarshaler = &_dummyGranularity

const (
	seconds = "seconds"
	minutes = "minutes"
	hours   = "hours"
)

var granularityNames = [...]string{
	seconds,
	minutes,
	hours,
}

var maxTimeGranularity = TimeGranularity(len(granularityNames) - 1)

func IsValidTimeGranularity(i TimeGranularity) bool {
	return i >= 0 && i <= maxTimeGranularity
}

func TimeGranularityFromName(s string) TimeGranularity {
	for idx, name := range granularityNames {
		if name == s {
			return TimeGranularity(idx)
		}
	}

	return UnknownTimeGranularity
}

func (tg TimeGranularity) String() string {
	if !IsValidTimeGranularity(tg) {
		return fmt.Sprintf("unknown(%d)", tg)
	}
	return granularityNames[tg]
}

func (tg *TimeGranularity) MarshalJSON() ([]byte, error) {
	if tg == nil {
		return nil, fmt.Errorf("cannot marshal unknown time granularity (nil)")
	}

	timeGran := *tg
	if !IsValidTimeGranularity(timeGran) {
		return nil, fmt.Errorf("cannot marshal unknown time granularity (%d)", timeGran)
	}

	name := granularityNames[timeGran]
	b := make([]byte, 0, len(name)+2)
	b = append(b, '"')
	b = append(b, name...)
	return append(b, '"'), nil
}

func (tg *TimeGranularity) UnmarshalJSON(bytes []byte) error {
	if tg == nil {
		return fmt.Errorf("cannot unmarshal into nil TimeGranularity")
	}

	length := len(bytes)
	if length <= 2 || bytes[0] != '"' || bytes[length-1] != '"' {
		return fmt.Errorf("cannot unmarshal invalid JSON: `%s`", string(bytes))
	}

	unmarshalName := string(bytes[1 : length-1])
	timeGran := TimeGranularityFromName(unmarshalName)
	if timeGran == UnknownTimeGranularity {
		return fmt.Errorf("cannot unmarshal unknown time granularity `%s`", unmarshalName)
	}

	*tg = timeGran
	return nil
}

func (tg *TimeGranularity) UnmarshalForm(value string) error {
	if tg == nil {
		return fmt.Errorf("cannot unmarshal into nil TimeGranularity")
	}

	timeGran := TimeGranularityFromName(value)
	if timeGran == UnknownTimeGranularity {
		return fmt.Errorf("cannot unmarshal unknown time granularity `%s`", value)
	}

	*tg = timeGran
	return nil

}
