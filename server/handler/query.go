package handler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/turbinelabs/api"
	"github.com/turbinelabs/server/handler"
	httperr "github.com/turbinelabs/server/http/error"
	tbntime "github.com/turbinelabs/time"
)

const oneHourInMicroseconds = int64(3600*time.Second) / int64(time.Microsecond)

type StatsTimeRange struct {
	// Start and End represent the start and end of a time range,
	// specified in microseconds since the Unix epoch, UTC. End
	// takes precedence over Duration.
	Start *int64 `json:"start"`
	End   *int64 `json:"end"`

	// Duration specifies how long a time span of stats data to
	// return in microseconds. End takes precedence over
	// Duration. If Start is specified, Duration sets the end of
	// the time span (e.g. from Start for Duration
	// microseconds). If Start is not specified, Duration sets the
	// start of the time span that many microseconds into the past
	// (e.g., Duration microseconds ago, until now).
	Duration *int64 `json:"duration"`
}

type StatsQueryTimeSeries struct {
	// Specifies the type of data returned. Required.
	QueryType QueryType `json:"query_type"`

	// Specifies the DomainKey for which stats are returned. If
	// not specified, stats are aggregated across domains.
	DomainKey *api.DomainKey `json:"domain_key"`

	// Specifies the RouteKey for which stats are returned. If
	// not specified, stats are aggregated across routes.
	RouteKey *api.RouteKey `json:"route_key"`

	// Specifies the HTTP method for which stats are returned. If
	// not specified, stats are aggregated across methods.
	Method *string `json:"method"`

	// Specifies the ClusterKey for which stats are returned. If
	// not specified, stats are aggregated across clusters.
	ClusterKey *api.ClusterKey `json:"cluster_key"`

	// Specifies the Instance keys (host:port) for which stats are
	// returned. If empty, stats are aggregated across all
	// instances. If one ore more instances are given, stats are
	// aggregated across only those instances.
	InstanceKeys []string `json:"instance_keys"`
}

type StatsQuery struct {
	// Specifies the ZoneKey for which stats are
	// queried. Required.
	ZoneKey api.ZoneKey `json:"zone_key"`

	// Specifies the time range of the query. Defaults to the last
	// hour.
	TimeRange StatsTimeRange `json:"time_range"`

	// Specifies one or more queries to execute against the given
	// zone and time range.
	TimeSeries []StatsQueryTimeSeries `json:"timeseries"`
}

type StatsPoint struct {
	// A data point.
	Value float64 `json:"value"`

	// Collection timestamp in microseconds since the Unix epoch,
	// UTC. N.B. that the actual resolution of the timestamp may
	// be less granular than microseconds.
	//
	// Microsecond resolution timestamps with an epoch of
	// 1970-01-01 00:00:00 reach 2^53 - 1, the maximum integer
	// exactly representable in Javascript, some time in 2255:
	// (2^53 - 1) / (86400 * 1000 * 1000)
	//     = 10249.99 days / 365.24
	//     = 285.42 years
	Timestamp int64 `json:"timestamp"`
}

type StatsTimeSeries struct {
	// The data points that represent the time series.
	Points []StatsPoint `json:"points"`
}

type StatsQueryResult struct {
	// Represents the timeseries returned by the query. The order
	// of returned TimeSeries values matches the order of the
	// original StatsQueryTimeSeries values in the request.
	TimeSeries []StatsTimeSeries `json:"timeseries"`
}

type QueryHandler interface {
	RunQuery(StatsQuery) (*StatsQueryResult, *httperr.Error)

	AsHandler() http.HandlerFunc
}

func NewQueryHandler() QueryHandler {
	return &queryHandler{}
}

type queryHandler struct{}

func validateQuery(q *StatsQuery) *httperr.Error {
	if q.ZoneKey == "" {
		return httperr.New400(
			"query requires zone_key",
			httperr.InvalidObjectErrorCode,
		)
	}

	for _, tsq := range q.TimeSeries {
		if !IsValidQueryType(tsq.QueryType) {
			return httperr.New400(
				fmt.Sprintf("query contains invalid query type %s", tsq.QueryType),
				httperr.InvalidObjectErrorCode,
			)
		}
	}

	return nil
}

func mkHandlerFunc(qh QueryHandler) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		rrw := handler.RichResponseWriter{rw}
		rr := handler.NewRichRequest(r)

		var result *StatsQueryResult

		statsQuery := StatsQuery{}
		err := handler.DecodeStruct("query", "query", rr, &statsQuery)
		if err == nil {
			result, err = qh.RunQuery(statsQuery)
		}

		rrw.WriteEnvelope(err, result)
	}
}

func normalizeTimeRange(tr StatsTimeRange) (int64, int64, *httperr.Error) {
	if tr.End != nil {
		if tr.Start != nil {
			start, end := *tr.Start, *tr.End
			if start > end {
				start, end = end, start
			} else if start == end {
				return 0, 0, httperr.New400(
					"empty time range: start equals end",
					httperr.MiscErrorCode,
				)
			}

			return start, end, nil
		} else {
			return 0, 0, httperr.New400(
				"time range start is not set",
				httperr.MiscErrorCode,
			)
		}
	} else if tr.Duration != nil {
		if tr.Start != nil {
			start := *tr.Start
			duration := *tr.Duration
			if duration > 0 {
				return start, start + duration, nil
			} else {
				return 0, 0, httperr.New400(
					"empty time range: duration is zero or negative",
					httperr.MiscErrorCode,
				)
			}
		} else {
			return 0, 0, httperr.New400(
				"time range start is not set",
				httperr.MiscErrorCode,
			)
		}
	} else if tr.Start != nil {
		return 0, 0, httperr.New400(
			"time range start is set, but not end or duration",
			httperr.MiscErrorCode,
		)
	} else {
		end := tbntime.ToUnixMicro(time.Now().Truncate(time.Second))
		start := end - oneHourInMicroseconds
		return start, end, nil
	}
}

func (qh *queryHandler) RunQuery(q StatsQuery) (*StatsQueryResult, *httperr.Error) {
	err := validateQuery(&q)
	if err != nil {
		return nil, err
	}

	return nil, httperr.New500("boom", httperr.UnknownUnclassifiedCode)
}

func (qh *queryHandler) AsHandler() http.HandlerFunc {
	return mkHandlerFunc(qh)
}