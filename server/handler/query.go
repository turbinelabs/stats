package handler

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/turbinelabs/api"
	apihttp "github.com/turbinelabs/api/http"
	httperr "github.com/turbinelabs/api/http/error"
	"github.com/turbinelabs/nonstdlib/executor"
	tbntime "github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/stats/server/handler/requestcontext"
)

const (
	oneHourInMicroseconds = int64(3600*time.Second) / int64(time.Microsecond)

	wavefrontAuthTokenHeader = "X-Auth-Token"
)

type StatsTimeRange struct {
	// Start and End represent the start and end of a time range,
	// specified in microseconds since the Unix epoch, UTC. End
	// takes precedence over Duration.
	Start *int64 `json:"start,omitempty" form:"start"`
	End   *int64 `json:"end,omitempty" form:"end"`

	// Duration specifies how long a time span of stats data to
	// return in microseconds. End takes precedence over
	// Duration. If Start is specified, Duration sets the end of
	// the time span (e.g. from Start for Duration
	// microseconds). If Start is not specified, Duration sets the
	// start of the time span that many microseconds into the past
	// (e.g., Duration microseconds ago, until now).
	Duration *int64 `json:"duration,omitempty" form:"duration"`

	// Granularity specifies how much time each data point
	// represents. If absent, it defaults to "seconds". Valid
	// values are "seconds", "minutes", or "hours".
	Granularity TimeGranularity `json:"granularity" form:"granularity"`
}

type StatsQueryTimeSeries struct {
	// Specifies a name for this timeseries query. It may be used
	// to assist in identifying the corresponding data in the
	// response object.
	Name string `json:"name,omitempty" form:"name"`

	// Specifies the type of data returned. Required.
	QueryType QueryType `json:"query_type" form:"query_type"`

	// Specifies the domain host for which stats are returned. The
	// host may be just a domain name (e.g., "example.com"), or a
	// domain name and port (e.g., "example.com:443"). The former
	// aggregates stats across all ports serving the domain. If
	// DomainHost is not specified, stats are aggregated across
	// all domains.
	DomainHost *string `json:"domain_host,omitempty" form:"domain_host"`

	// Specifies the RouteKey for which stats are returned. If
	// not specified, stats are aggregated across routes.
	RouteKey *api.RouteKey `json:"route_key,omitempty" form:"route_key"`

	// Specifies the SharedRule name for which stats are
	// returned. If not specified, stats are aggregated across
	// shared rules.
	SharedRuleName *string `json:"shared_rule_name,omitempty" form:"shared_rule_name"`

	// Specifies the RuleKey for which stats are returned.
	// Requires that a RouteKey or SharedRuleName is given. If not
	// specified, stats are aggregated across rules.
	RuleKey *api.RuleKey `json:"rule_key,omitempty" form:"rule_key"`

	// Specifies the HTTP method for which stats are returned. If
	// not specified, stats are aggregated across methods.
	Method *string `json:"method,omitempty" form:"method"`

	// Specifies the Cluster name for which stats are returned. If
	// not specified, stats are aggregated across clusters.
	ClusterName *string `json:"cluster_name,omitempty" form:"cluster_name"`

	// Specifies the Instance keys (host:port) for which stats are
	// returned. If empty, stats are aggregated across all
	// instances. If one ore more instances are given, stats are
	// aggregated across only those instances.
	InstanceKeys []string `json:"instance_keys,omitempty" form:"instance_keys"`
}

type StatsQuery struct {
	// Specifies the zone name for which stats are
	// queried. Required.
	ZoneName string `json:"zone_name" form:"zone_name"`

	// Specifies the time range of the query. Defaults to the last
	// hour.
	TimeRange StatsTimeRange `json:"time_range" form:"time_range"`

	// Specifies one or more queries to execute against the given
	// zone and time range.
	TimeSeries []StatsQueryTimeSeries `json:"timeseries" form:"timeseries"`
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
	// The StatsQueryTimeSeries object corresponding to the data
	// points.
	Query StatsQueryTimeSeries `json:"query"`

	// The data points that represent the time series.
	Points []StatsPoint `json:"points"`
}

type StatsQueryResult struct {
	// The StatsTimeRange used to issue this query. The object is
	// normalized such that all of its fields are set and
	// consistent.
	TimeRange StatsTimeRange `json:"time_range"`

	// Represents the timeseries returned by the query. The order
	// of returned TimeSeries values matches the order of the
	// original StatsQueryTimeSeries values in the request.
	TimeSeries []StatsTimeSeries `json:"timeseries"`
}

type QueryHandler interface {
	RunQuery(api.OrgKey, StatsQuery) (*StatsQueryResult, *httperr.Error)

	AsHandler() http.HandlerFunc
}

// Constructs a new QueryHandler from the given wavefrontServerUrl
// (e.g., "https://metrics.wavefront.com") and wavefrontApiToken.
func NewQueryHandler(
	wavefrontServerUrl string,
	wavefrontApiToken string,
	verboseLogging bool,
	exec executor.Executor,

) (QueryHandler, error) {
	queryBuilder, err := newWavefrontQueryBuilder(wavefrontServerUrl)
	if err != nil {
		return nil, err
	}

	return &queryHandler{
		wavefrontApiToken: wavefrontApiToken,
		client:            apihttp.HeaderPreservingClient(),
		formatQueryUrl:    queryBuilder.FormatWavefrontQueryUrl,
		verboseLogging:    verboseLogging,
		exec:              exec,
	}, nil
}

type queryFormatter func(
	int64,
	int64,
	TimeGranularity,
	api.OrgKey,
	string,
	*StatsQueryTimeSeries,
) string

type queryHandler struct {
	wavefrontApiToken string
	client            *http.Client
	formatQueryUrl    queryFormatter
	verboseLogging    bool
	exec              executor.Executor
}

func validateQuery(q *StatsQuery) *httperr.Error {
	if q.ZoneName == "" {
		return httperr.New400(
			"query requires zone_name",
			httperr.InvalidObjectErrorCode,
		)
	}

	if !IsValidTimeGranularity(q.TimeRange.Granularity) {
		return httperr.New400(
			fmt.Sprintf(
				"query contains invalid time granularity %s",
				q.TimeRange.Granularity,
			),
			httperr.InvalidObjectErrorCode,
		)
	}

	nameOrIndex := func(name string, idx int) string {
		if name == "" {
			return fmt.Sprintf("[%d]", idx)
		} else {
			return fmt.Sprintf(" '%s'", name)
		}
	}

	for idx, tsq := range q.TimeSeries {
		if !IsValidQueryType(tsq.QueryType) {
			return httperr.New400(
				fmt.Sprintf(
					"query%s contains invalid query type %s",
					nameOrIndex(tsq.Name, idx),
					tsq.QueryType,
				),
				httperr.InvalidObjectErrorCode,
			)
		}

		if tsq.RuleKey != nil && tsq.RouteKey == nil && tsq.SharedRuleName == nil {
			return httperr.New400(
				fmt.Sprintf(
					"query%s must have a RouteKey and/or SharedRuleName to scope the given RuleKey",
					nameOrIndex(tsq.Name, idx),
				),
				httperr.InvalidObjectErrorCode,
			)
		}
	}

	return nil
}

func mkHandlerFunc(qh QueryHandler) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		rrw := apihttp.RichResponseWriter{rw}
		rr := apihttp.NewRichRequest(r)

		var result *StatsQueryResult
		var err *httperr.Error

		requestContext := requestcontext.New(r)
		if orgKey, ok := requestContext.GetOrgKey(); ok {
			statsQuery := StatsQuery{}
			err = QueryDecoder.DecodeQuery(rr, &statsQuery)
			if err == nil {
				result, err = qh.RunQuery(orgKey, statsQuery)
			}
		} else {
			err = httperr.New500("authorization config error", httperr.MiscErrorCode)
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
			end := tbntime.ToUnixMicro(time.Now().Truncate(time.Second))
			start := end - *tr.Duration
			return start, end, nil
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

func (qh *queryHandler) runQueries(urls []string) ([]StatsTimeSeries, *httperr.Error) {
	requestFuncs := make([]executor.Func, len(urls))
	for i, url := range urls {
		request, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, httperr.New500(err.Error(), httperr.MiscErrorCode)
		}

		request.Header.Add(wavefrontAuthTokenHeader, qh.wavefrontApiToken)

		requestFuncs[i] = func(ctxt context.Context) (interface{}, error) {
			return qh.client.Do(request.WithContext(ctxt))
		}
	}

	result := make(chan executor.Try, 1)

	qh.exec.ExecGathered(requestFuncs, func(try executor.Try) {
		result <- try
	})

	try := <-result
	if try.IsError() {
		return nil, httperr.New500(try.Error().Error(), httperr.MiscErrorCode)
	}

	responses := try.Get().([]interface{})
	results := make([]StatsTimeSeries, len(responses))
	for idx, riface := range responses {
		r := riface.(*http.Response)

		if r.StatusCode >= http.StatusBadRequest {
			defer r.Body.Close()
			var responseBody string
			if body, readErr := ioutil.ReadAll(r.Body); readErr == nil {
				responseBody = string(body)
			} else {
				responseBody =
					"(no error details available: could not read response body)"
			}
			return nil, httperr.New500(
				fmt.Sprintf(
					"Error %d querying wavefront: %s",
					r.StatusCode,
					responseBody,
				),
				httperr.MiscErrorCode,
			)
		}

		ts, err := decodeWavefrontResponse(r)
		if err != nil {
			return nil, err
		}
		results[idx] = ts
	}

	return results, nil
}

func makeQueryResult(
	start, end int64,
	granularity TimeGranularity,
	queryTimeSeries []StatsQueryTimeSeries,
	resultTimeSeries []StatsTimeSeries,
) (*StatsQueryResult, *httperr.Error) {
	if len(queryTimeSeries) != len(resultTimeSeries) {
		return nil, httperr.New500(
			"Mismatched query and response",
			httperr.MiscErrorCode,
		)
	}

	for idx := range resultTimeSeries {
		resultTimeSeries[idx].Query = queryTimeSeries[idx]
	}

	duration := end - start
	responseTimeRange := StatsTimeRange{
		Start:       &start,
		End:         &end,
		Duration:    &duration,
		Granularity: granularity,
	}

	return &StatsQueryResult{TimeRange: responseTimeRange, TimeSeries: resultTimeSeries}, nil
}

func (qh *queryHandler) RunQuery(
	orgKey api.OrgKey,
	q StatsQuery,
) (*StatsQueryResult, *httperr.Error) {
	if err := validateQuery(&q); err != nil {
		return nil, err
	}

	start, end, herr := normalizeTimeRange(q.TimeRange)
	if herr != nil {
		return nil, herr
	}

	queryUrls := make([]string, len(q.TimeSeries))
	for idx, qts := range q.TimeSeries {
		url := qh.formatQueryUrl(
			start,
			end,
			q.TimeRange.Granularity,
			orgKey,
			q.ZoneName,
			&qts,
		)

		if qh.verboseLogging {
			fmt.Println(url)
		}

		queryUrls[idx] = url
	}

	tsResponse, err := qh.runQueries(queryUrls)
	if err != nil {
		return nil, err
	}

	return makeQueryResult(start, end, q.TimeRange.Granularity, q.TimeSeries, tsResponse)
}

func (qh *queryHandler) AsHandler() http.HandlerFunc {
	return mkHandlerFunc(qh)
}
