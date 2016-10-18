package test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/turbinelabs/api"
	"github.com/turbinelabs/test/assert"
)

const accessLogTemplate = `{{Timestamp .OffsetSeconds}} {{.RequestId}} {{.StatusCode}} {{.RemoteIP}} {{.Domain}} {{.Method}} {{.Path}} {{.RequestLength}} {{.ContentLength}} {{.BytesSent}} {{.BodyBytesSent}} {{.RequestTime}} "{{.HTTPReferrer}}" "{{.HTTPUserAgent}}"`

type accessLogEntry struct {
	OffsetSeconds int
	RequestId     string
	StatusCode    int
	RemoteIP      string
	Domain        api.DomainKey
	Method        string
	Path          string
	RequestTime   float64
}

func (e *accessLogEntry) RequestLength() int    { return 1 }
func (e *accessLogEntry) ContentLength() int    { return 2 }
func (e *accessLogEntry) BytesSent() int        { return 3 }
func (e *accessLogEntry) BodyBytesSent() int    { return 4 }
func (e *accessLogEntry) HTTPReferrer() string  { return "referrinator" }
func (e *accessLogEntry) HTTPUserAgent() string { return "larry-moe-and-curly" }

func genId() string {
	return fmt.Sprintf("%08x%08x", rand.Int63n(0xFFFFFFFF), rand.Int63n(0xFFFFFFFF))
}

var (
	localhost = "127.0.0.1"
	domain1   = api.DomainKey("www.example.com")
	domain2   = api.DomainKey("example.org")
	path1     = "/"
	path2     = "/users"
)

var accessLogEntries = []interface{}{
	&accessLogEntry{0, genId(), 200, localhost, domain1, "GET", path1, 0.001},
	&accessLogEntry{1, genId(), 200, localhost, domain1, "GET", path2, 0.002},
	&accessLogEntry{2, genId(), 200, localhost, domain2, "GET", path1, 0.004},
	&accessLogEntry{3, genId(), 200, localhost, domain2, "GET", path2, 0.008},
	&accessLogEntry{4, genId(), 200, localhost, domain1, "POST", path1, 0.016},
	&accessLogEntry{5, genId(), 200, localhost, domain1, "POST", path2, 0.032},
	&accessLogEntry{6, genId(), 200, localhost, domain2, "POST", path1, 0.064},
	&accessLogEntry{7, genId(), 200, localhost, domain2, "POST", path2, 0.128},
	&accessLogEntry{8, genId(), 200, localhost, domain1, "GET", path1, 0.256},
	&accessLogEntry{9, genId(), 200, localhost, domain1, "GET", path2, 0.512},
	&accessLogEntry{10, genId(), 200, localhost, domain2, "GET", path1, 1.024},
	&accessLogEntry{11, genId(), 200, localhost, domain2, "GET", path2, 2.048},
	&accessLogEntry{12, genId(), 200, localhost, domain1, "DELETE", path1, 4.096},
	&accessLogEntry{13, genId(), 200, localhost, domain1, "DELETE", path2, 8.192},
	&accessLogEntry{14, genId(), 200, localhost, domain2, "DELETE", path1, 16.384},
	&accessLogEntry{15, genId(), 200, localhost, domain2, "DELETE", path2, 32.768},
	&accessLogEntry{16, genId(), 400, localhost, domain1, "GET", path1, 65.536},
	&accessLogEntry{17, genId(), 404, localhost, domain1, "GET", path2, 131.072},
	&accessLogEntry{18, genId(), 500, localhost, domain2, "GET", path1, 262.144},
	&accessLogEntry{19, genId(), 503, localhost, domain2, "GET", path2, 524.288},
}

func TestZoneLevelStats(t *testing.T) {
	harness := NewStatsServerTestHarness()
	harness.WriteAccessLogFile(t, accessLogTemplate, accessLogEntries)
	if err := harness.Start(); err != nil {
		t.Fatalf("test harness failed to start: %s", err.Error())
	}
	defer harness.Stop()

	minStats, maxStats := harness.MockWavefrontApi().CountPoints()
	for minStats < 0 || maxStats < 20 {
		time.Sleep(1 * time.Second)
		minStats, maxStats = harness.MockWavefrontApi().CountPoints()
	}

	assert.Equal(t, maxStats, 20)

	metrics := harness.MockWavefrontApi().MetricNames()
	sort.Strings(metrics)

	// We do not presently have a way to query these from the
	// stats API, but they are forwarded.
	assert.ArrayEqual(
		t,
		metrics,
		[]string{
			TestOrgKey + ".latency",
			TestOrgKey + ".requests",
			TestOrgKey + ".responses.200",
			TestOrgKey + ".responses.400",
			TestOrgKey + ".responses.404",
			TestOrgKey + ".responses.500",
			TestOrgKey + ".responses.503",
		},
	)
}
