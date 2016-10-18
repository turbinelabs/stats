package test

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/turbinelabs/api"
	"github.com/turbinelabs/ptr"
	"github.com/turbinelabs/stats/server/handler"
	"github.com/turbinelabs/test/assert"
	tbntime "github.com/turbinelabs/time"
)

const upstreamLogTemplate = `{{Timestamp .OffsetSeconds}} {{.RequestId}} {{.StatusCode}} "{{.Instance}}" {{.InstanceAddr}} {{.ResponseLength}} {{.ResponseTime}} {{.ConnectTime}} {{.HeaderTime}} "{{.Upstream}}" {{.InstanceMetadata}} "{{.Domain}}" "{{.Route}}" {{.Method}} "{{.Rule}}" "{{.SharedRule}}"`

type upstreamLogEntry struct {
	OffsetSeconds int
	RequestId     string
	StatusCode    int
	Port          int
	ResponseTime  float64
	Upstream      api.ClusterKey
	Domain        api.DomainKey
	Route         api.RouteKey
	Method        string
	Rule          api.RuleKey
}

func (e *upstreamLogEntry) Instance() string         { return fmt.Sprintf("localhost:%d", e.Port) }
func (e *upstreamLogEntry) InstanceAddr() string     { return fmt.Sprintf("127.0.0.1:%d", e.Port) }
func (e *upstreamLogEntry) ResponseLength() int      { return 100 }
func (e *upstreamLogEntry) ConnectTime() float64     { return 0.00001 }
func (e *upstreamLogEntry) HeaderTime() float64      { return 0.00001 }
func (e *upstreamLogEntry) InstanceMetadata() string { return "x=y&a=b" }
func (e *upstreamLogEntry) SharedRule() string       { return "sharing-is-caring" }

var (
	p1   = 8000
	p2   = 9000
	u1   = api.ClusterKey("upstream1")
	u2   = api.ClusterKey("upstream2")
	rte1 = api.RouteKey("route-key-1")
	rte2 = api.RouteKey("route-key-2")
	r1   = api.RuleKey("rule1")
	r2   = api.RuleKey("rule2")
)

var upstreamLogEntries = []interface{}{
	&upstreamLogEntry{0, genId(), 200, p1, 0.001, u1, domain1, rte1, "GET", r1},
	&upstreamLogEntry{0, genId(), 201, p2, 0.001, u1, domain1, rte1, "GET", r1},
	&upstreamLogEntry{0, genId(), 500, p1, 0.001, u1, domain1, rte1, "GET", r1},
	&upstreamLogEntry{0, genId(), 204, p2, 0.001, u1, domain1, rte1, "GET", r1},
	&upstreamLogEntry{0, genId(), 404, p2, 0.001, u1, domain1, rte1, "POST", r1},

	&upstreamLogEntry{1, genId(), 100, p1, 0.001, u1, domain1, rte1, "GET", r1},
	&upstreamLogEntry{2, genId(), 301, p2, 0.001, u1, domain1, rte1, "GET", r1},

	&upstreamLogEntry{3, genId(), 200, p1, 0.001, u1, domain1, rte1, "POST", r1},
	&upstreamLogEntry{4, genId(), 200, p2, 0.001, u1, domain1, rte2, "POST", r1},
	&upstreamLogEntry{5, genId(), 200, p1, 0.001, u1, domain1, rte1, "POST", r1},
	&upstreamLogEntry{6, genId(), 200, p2, 0.001, u1, domain1, rte2, "POST", r1},
	&upstreamLogEntry{7, genId(), 200, p1, 0.001, u2, domain1, rte1, "POST", r1},
	&upstreamLogEntry{8, genId(), 200, p2, 0.001, u2, domain1, rte2, "POST", r1},
	&upstreamLogEntry{9, genId(), 200, p1, 0.001, u2, domain1, rte1, "POST", r1},
	&upstreamLogEntry{10, genId(), 200, p2, 0.001, u2, domain1, rte2, "POST", r1},

	&upstreamLogEntry{11, genId(), 200, p1, 0.001, u1, domain1, rte1, "GET", r2},
	&upstreamLogEntry{12, genId(), 200, p2, 0.001, u1, domain1, rte2, "GET", r2},
	&upstreamLogEntry{13, genId(), 200, p1, 0.001, u1, domain1, rte1, "GET", r2},
	&upstreamLogEntry{14, genId(), 200, p2, 0.001, u1, domain1, rte2, "GET", r2},
	&upstreamLogEntry{15, genId(), 200, p1, 0.001, u2, domain2, rte1, "GET", r2},
	&upstreamLogEntry{16, genId(), 200, p2, 0.001, u2, domain2, rte2, "GET", r2},
	&upstreamLogEntry{17, genId(), 200, p1, 0.001, u2, domain2, rte1, "GET", r2},
	&upstreamLogEntry{18, genId(), 200, p2, 0.001, u2, domain2, rte2, "GET", r2},
}

func TestUnfilteredDetailedStats(t *testing.T) {
	harness := NewStatsServerTestHarness()
	harness.WriteUpstreamLogFile(t, upstreamLogTemplate, upstreamLogEntries)
	if err := harness.Start(); err != nil {
		t.Fatalf("test harness failed to start: %s", err.Error())
	}
	defer harness.Stop()

	n := harness.MockWavefrontApi().TotalPoints()
	for n < len(upstreamLogEntries)*3 {
		time.Sleep(1 * time.Second)
		n = harness.MockWavefrontApi().TotalPoints()
	}

	metrics := harness.MockWavefrontApi().MetricNames()
	sort.Strings(metrics)
	fmt.Println(metrics)

	start := tbntime.ToUnixMicro(harness.LogStartTime)
	duration := (19 * time.Second).Nanoseconds() / 1000

	query := handler.StatsQuery{
		ZoneName: TestZoneName,
		TimeRange: handler.StatsTimeRange{
			Start:       &start,
			Duration:    &duration,
			Granularity: handler.Seconds,
		},
		TimeSeries: []handler.StatsQueryTimeSeries{
			{Name: "req", QueryType: handler.Requests},
			//			{Name: "resp", QueryType: handler.Responses}, // pending #1433
			{Name: "ok", QueryType: handler.SuccessfulResponses},
			{Name: "err", QueryType: handler.ErrorResponses},
			{Name: "fail", QueryType: handler.FailureResponses},
			{Name: "sr", QueryType: handler.SuccessRate},
			//			{Name: "p50", QueryType: handler.LatencyP50}, // pending #1403
			//			{Name: "p99", QueryType: handler.LatencyP99},
		},
	}

	result, err := harness.Query(&query)
	if err != nil {
		t.Fatalf("query error: %s", err.Error())
		return
	}
	assert.NonNil(t, result)

	expected := [][]float64{
		{5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, // req
		//{5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, // resp
		{3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},   // ok
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},   // err
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},   // fail
		{0.6, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, // sr
		//{}, // p50
		//{}, // p99
	}

	for idx, result := range result.TimeSeries {
		assert.Group(result.Query.Name, t, func(g *assert.G) {
			assert.Equal(g, len(result.Points), 19)
			assert.Equal(g, result.Points[0].Timestamp, start)
			assert.Equal(g, result.Points[18].Timestamp, start+duration-1000000)

			values := make([]float64, 19)
			for i, pt := range result.Points {
				values[i] = pt.Value
			}

			assert.ArrayEqual(g, values, expected[idx])
		})
	}
}

func TestFilteredDetailedStats(t *testing.T) {
	harness := NewStatsServerTestHarness()
	harness.WriteUpstreamLogFile(t, upstreamLogTemplate, upstreamLogEntries)
	if err := harness.Start(); err != nil {
		t.Fatalf("test harness failed to start: %s", err.Error())
	}
	defer harness.Stop()

	n := harness.MockWavefrontApi().TotalPoints()
	for n < len(upstreamLogEntries)*3 {
		time.Sleep(1 * time.Second)
		n = harness.MockWavefrontApi().TotalPoints()
	}

	metrics := harness.MockWavefrontApi().MetricNames()
	sort.Strings(metrics)
	fmt.Println(metrics)

	start := tbntime.ToUnixMicro(harness.LogStartTime)
	duration := (19 * time.Second).Nanoseconds() / 1000

	query := handler.StatsQuery{
		ZoneName: TestZoneName,
		TimeRange: handler.StatsTimeRange{
			Start:       &start,
			Duration:    &duration,
			Granularity: handler.Seconds,
		},
		TimeSeries: []handler.StatsQueryTimeSeries{
			{
				Name:      "d1",
				QueryType: handler.Requests,
				DomainKey: &domain1,
			},
			{
				Name:      "d2",
				QueryType: handler.Requests,
				DomainKey: &domain2,
			},
			{
				Name:      "rte1",
				QueryType: handler.Requests,
				RouteKey:  &rte1,
			},
			{
				Name:      "d1-rte1",
				QueryType: handler.Requests,
				DomainKey: &domain1,
				RouteKey:  &rte1,
			},
			{
				Name:         "p1",
				QueryType:    handler.Requests,
				InstanceKeys: []string{fmt.Sprintf("localhost:%d", p1)},
			},
			{
				Name:         "p2",
				QueryType:    handler.Requests,
				InstanceKeys: []string{fmt.Sprintf("localhost:%d", p2)},
			},
			{
				Name:       "u1",
				QueryType:  handler.Requests,
				ClusterKey: &u1,
			},
			{
				Name:      "method",
				QueryType: handler.Requests,
				Method:    ptr.String("POST"),
			},
			{
				Name:           "r1",
				QueryType:      handler.Requests,
				SharedRuleName: ptr.String("sharing-is-caring"),
				RuleKey:        &r1,
			},
			{
				Name:           "shared-rule-yup",
				QueryType:      handler.Requests,
				SharedRuleName: ptr.String("sharing-is-caring"),
			},
			{
				Name:           "shared-rule-nope",
				QueryType:      handler.Requests,
				SharedRuleName: ptr.String("NOPE"),
			},
			{
				Name:         "combo-move",
				QueryType:    handler.Requests,
				DomainKey:    &domain1,
				RouteKey:     &rte1,
				Method:       ptr.String("GET"),
				ClusterKey:   &u1,
				InstanceKeys: []string{fmt.Sprintf("localhost:%d", p1)},
			},
		},
	}

	result, err := harness.Query(&query)
	if err != nil {
		t.Fatalf("query error: %s", err.Error())
		return
	}
	assert.NonNil(t, result)

	expected := [][]float64{
		{5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0}, // d1
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1}, // d2
		{5, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0}, // rte1
		{5, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0}, // d1-rte1
		{2, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0}, // p1
		{3, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1}, // p2
		{5, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0}, // u1
		{1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0}, // method
		{5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0}, // r1
		{5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, // shared-rule-yep
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // shared-rule-nope
		{2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0}, // combo-move
	}

	for idx, result := range result.TimeSeries {
		assert.Group(result.Query.Name, t, func(g *assert.G) {
			assert.Equal(g, len(result.Points), 19)
			assert.Equal(g, result.Points[0].Timestamp, start)
			assert.Equal(g, result.Points[18].Timestamp, start+duration-1000000)

			values := make([]float64, 19)
			for i, pt := range result.Points {
				values[i] = pt.Value
			}

			assert.ArrayEqual(g, values, expected[idx])
		})
	}
}
