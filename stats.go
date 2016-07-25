package stats

import (
	"time"
)

const (
	millisPerSecond = int64(1000)
	millisPerNano   = int64(time.Millisecond)
)

// StatsPayload is the payload of a stats update call.
type StatsPayload struct {
	Source string `json:"source"`
	Stats  []Stat `json:"stats"`
}

type Stat struct {
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Timestamp int64             `json:"timestamp"` // milliseconds since the Unix epoch, UTC
	Tags      map[string]string `json:"tags"`
}

// Result is a JSON-encodable struct that encapsulates the result of
// forwarding metrics.
type Result struct {
	NumAccepted int `json:"numAccepted"`
}

func TimeFromMilliseconds(millis int64) *time.Time {
	t := time.Unix(millis/millisPerSecond, millis%1000*millisPerNano).In(time.UTC)
	return &t
}

func TimeToMilliseconds(t *time.Time) int64 {
	return t.UnixNano() / millisPerNano
}
