package stats

import (
	"testing"
	"time"

	"github.com/turbinelabs/test/assert"
)

func TestTimeFromMilliseconds(t *testing.T) {
	millis := int64(1468970983150)
	ts := TimeFromMilliseconds(millis)
	expected, err := time.Parse(time.RFC3339Nano, "2016-07-19T23:29:43.150000000Z")
	assert.Nil(t, err)

	assert.DeepEqual(t, ts, &expected)
}

func TestTimeToMilliseconds(t *testing.T) {
	ts, err := time.Parse(time.RFC3339Nano, "2016-07-19T23:29:43.150000000Z")
	assert.Nil(t, err)

	expected := int64(1468970983150)

	millis := TimeToMilliseconds(&ts)

	assert.Equal(t, millis, expected)
}
