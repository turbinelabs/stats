package handler

import (
	"testing"

	"github.com/turbinelabs/api/service/stats/timegranularity"
	"github.com/turbinelabs/test/assert"
)

func TestUnmarshalTimeGranularityFromForm(t *testing.T) {
	tg, err := unmarshalTimeGranularityFromForm(nil)
	assert.Nil(t, tg)
	assert.DeepEqual(t, err, noTimeGranularityDecodeError)

	tg, err = unmarshalTimeGranularityFromForm([]string{})
	assert.Nil(t, tg)
	assert.DeepEqual(t, err, noTimeGranularityDecodeError)

	tg, err = unmarshalTimeGranularityFromForm([]string{"minutes"})
	assert.Nil(t, err)
	assert.Equal(t, tg, timegranularity.Minutes)
}
