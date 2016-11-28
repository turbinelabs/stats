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
	statsapi "github.com/turbinelabs/api/service/stats"
	"github.com/turbinelabs/api/service/stats/querytype"
	"github.com/turbinelabs/api/service/stats/timegranularity"
	"github.com/turbinelabs/nonstdlib/executor"
	tbntime "github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/stats/server/handler/requestcontext"
)

const (
	oneHourInMicroseconds = int64(3600*time.Second) / int64(time.Microsecond)

	wavefrontAuthTokenHeader = "X-Auth-Token"
)

type QueryHandler interface {
	RunQuery(api.OrgKey, statsapi.Query) (*statsapi.QueryResult, *httperr.Error)

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
	timegranularity.TimeGranularity,
	api.OrgKey,
	string,
	*statsapi.QueryTimeSeries,
) string

type queryHandler struct {
	wavefrontApiToken string
	client            *http.Client
	formatQueryUrl    queryFormatter
	verboseLogging    bool
	exec              executor.Executor
}

func validateQuery(q *statsapi.Query) *httperr.Error {
	if q.ZoneName == "" {
		return httperr.New400(
			"query requires zone_name",
			httperr.InvalidObjectErrorCode,
		)
	}

	if !timegranularity.IsValid(q.TimeRange.Granularity) {
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
		if !querytype.IsValid(tsq.QueryType) {
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

		var result *statsapi.QueryResult
		var err *httperr.Error

		requestContext := requestcontext.New(r)
		if orgKey, ok := requestContext.GetOrgKey(); ok {
			statsQuery := statsapi.Query{}
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

func normalizeTimeRange(tr statsapi.TimeRange) (int64, int64, *httperr.Error) {
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

func (qh *queryHandler) runQueries(urls []string) ([]statsapi.TimeSeries, *httperr.Error) {
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
	results := make([]statsapi.TimeSeries, len(responses))
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
	granularity timegranularity.TimeGranularity,
	queryTimeSeries []statsapi.QueryTimeSeries,
	resultTimeSeries []statsapi.TimeSeries,
) (*statsapi.QueryResult, *httperr.Error) {
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
	responseTimeRange := statsapi.TimeRange{
		Start:       &start,
		End:         &end,
		Duration:    &duration,
		Granularity: granularity,
	}

	return &statsapi.QueryResult{TimeRange: responseTimeRange, TimeSeries: resultTimeSeries}, nil
}

func (qh *queryHandler) RunQuery(
	orgKey api.OrgKey,
	q statsapi.Query,
) (*statsapi.QueryResult, *httperr.Error) {
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
