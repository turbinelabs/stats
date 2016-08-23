package handler

import (
	"testing"

	"github.com/turbinelabs/test/assert"
)

func TestUnmarshalQueryTypeFromForm(t *testing.T) {
	qt, err := unmarshalQueryTypeFromForm(nil)
	assert.Nil(t, qt)
	assert.DeepEqual(t, err, noQueryTypeDecodeError)

	qt, err = unmarshalQueryTypeFromForm([]string{})
	assert.Nil(t, qt)
	assert.DeepEqual(t, err, noQueryTypeDecodeError)

	qt, err = unmarshalQueryTypeFromForm([]string{"requests"})
	assert.Nil(t, err)
	assert.Equal(t, qt, Requests)
}
