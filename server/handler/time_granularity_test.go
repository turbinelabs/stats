package handler

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/turbinelabs/test/assert"
)

func forEachTimeGranularity(f func(TimeGranularity)) {
	for i := 1; i <= int(maxTimeGranularity); i++ {
		tg := TimeGranularity(i)
		f(tg)
	}
}

type testTGStruct struct {
	TimeGranularity TimeGranularity `json:"time_granularity"`
}

func TestTimeGranularityString(t *testing.T) {
	assert.Equal(t, Requests.String(), "requests")
	assert.Equal(t, UnknownTimeGranularity.String(), "unknown(3)")
	assert.Equal(t, TimeGranularity(100).String(), "unknown(100)")
}

func TestIsValidTimeGranularity(t *testing.T) {
	invalid := []TimeGranularity{
		TimeGranularity(-1),
		TimeGranularity(maxTimeGranularity + 1),
	}

	for _, tg := range invalid {
		assert.False(t, IsValidTimeGranularity(tg))
	}

	forEachTimeGranularity(func(tg TimeGranularity) {
		assert.True(t, IsValidTimeGranularity(tg))
	})
}

func TestTimeGranularityFromName(t *testing.T) {
	validTimeGranularitys := map[TimeGranularity]string{
		Seconds: seconds,
		Minutes: minutes,
		Hours:   hours,
	}

	for expectedTg, name := range validTimeGranularitys {
		tg := TimeGranularityFromName(name)
		assert.Equal(t, tg, expectedTg)
	}

	invalidTimeGranularitys := []string{"bob", "unknown", "1"}

	for _, name := range invalidTimeGranularitys {
		tg := TimeGranularityFromName(name)
		assert.Equal(t, tg, UnknownTimeGranularity)
	}
}

func TestTimeGranularityMarshalJSON(t *testing.T) {
	timeGranularities := map[TimeGranularity]string{
		Seconds: seconds,
		Minutes: minutes,
		Hours:   hours,
	}

	for timeGranularity, name := range timeGranularities {
		bytes, err := timeGranularity.MarshalJSON()
		assert.Nil(t, err)
		expected := []byte(fmt.Sprintf(`"%s"`, name))
		assert.DeepEqual(t, bytes, expected)
	}
}

func TestTimeGranularityMarshalJSONUnknown(t *testing.T) {
	unknownTimeGranularitys := []TimeGranularity{
		UnknownTimeGranularity,
		TimeGranularity(maxTimeGranularity + 1),
	}

	for _, unknownTimeGranularity := range unknownTimeGranularitys {
		bytes, err := unknownTimeGranularity.MarshalJSON()
		assert.Nil(t, bytes)
		assert.ErrorContains(t, err, "cannot marshal unknown time granularity")
	}
}

func TestTimeGranularityMarshalJSONNil(t *testing.T) {
	var timeGranularity *TimeGranularity

	bytes, err := timeGranularity.MarshalJSON()
	assert.ErrorContains(t, err, "cannot marshal unknown time granularity (nil)")
	assert.Nil(t, bytes)
}

func TestTimeGranularityUnmarshalJSON(t *testing.T) {
	quoted := func(s string) string {
		return fmt.Sprintf(`"%s"`, s)
	}

	timeGranularities := map[string]TimeGranularity{
		quoted(seconds): Seconds,
		quoted(minutes): Minutes,
		quoted(hours):   Hours,
	}

	for data, expectedTimeGranularity := range timeGranularities {
		var timeGranularity TimeGranularity

		err := timeGranularity.UnmarshalJSON([]byte(data))
		assert.Nil(t, err)
		assert.Equal(t, timeGranularity, expectedTimeGranularity)
	}
}

func TestTimeGranularityUnmarshalJSONUnknown(t *testing.T) {
	unknownTimeGranularitys := []string{`"unknown"`, `"nope"`}

	for _, unknownName := range unknownTimeGranularitys {
		var timeGranularity TimeGranularity

		err := timeGranularity.UnmarshalJSON([]byte(unknownName))
		assert.ErrorContains(t, err, "cannot unmarshal unknown time granularity")
	}
}

func TestTimeGranularityUnmarshalJSONNil(t *testing.T) {
	var timeGranularity *TimeGranularity

	err := timeGranularity.UnmarshalJSON([]byte(`"seconds"`))
	assert.ErrorContains(t, err, "cannot unmarshal into nil TimeGranularity")
}

func TestTimeGranularityUnmarshalJSONInvalid(t *testing.T) {
	invalidTimeGranularitys := []string{``, `"`, `x`, `xx`, `"x`, `x"`, `'something'`}

	for _, invalidName := range invalidTimeGranularitys {
		var timeGranularity TimeGranularity

		err := timeGranularity.UnmarshalJSON([]byte(invalidName))
		assert.ErrorContains(t, err, "cannot unmarshal invalid JSON")
	}
}

func TestTimeGranularityUnmarshalForm(t *testing.T) {
	timeGranularities := map[string]TimeGranularity{
		seconds: Seconds,
		minutes: Minutes,
		hours:   Hours,
	}

	for data, expectedTimeGranularity := range timeGranularities {
		var timeGranularity TimeGranularity

		err := timeGranularity.UnmarshalForm(data)
		assert.Nil(t, err)
		assert.Equal(t, timeGranularity, expectedTimeGranularity)
	}
}

func TestTimeGranularityUnmarshalFormUnknown(t *testing.T) {
	unknownTimeGranularitys := []string{`unknown`, `nope`}

	for _, unknownName := range unknownTimeGranularitys {
		var timeGranularity TimeGranularity

		err := timeGranularity.UnmarshalForm(unknownName)
		assert.ErrorContains(t, err, "cannot unmarshal unknown time granularity")
	}
}

func TestTimeGranularityUnmarshalFormNil(t *testing.T) {
	var timeGranularity *TimeGranularity

	err := timeGranularity.UnmarshalForm(`requests`)
	assert.ErrorContains(t, err, "cannot unmarshal into nil TimeGranularity")
}

func TestTimeGranularityRoundTripStruct(t *testing.T) {
	expected := testTGStruct{TimeGranularity: Hours}

	bytes, err := json.Marshal(&expected)
	assert.Nil(t, err)
	assert.NonNil(t, bytes)
	assert.Equal(t, string(bytes), `{"time_granularity":"hours"}`)

	var ts testTGStruct
	err = json.Unmarshal(bytes, &ts)
	assert.Nil(t, err)
	assert.Equal(t, ts, expected)
}
