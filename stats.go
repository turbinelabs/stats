package stats

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
