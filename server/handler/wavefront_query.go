package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/turbinelabs/api"
	httperr "github.com/turbinelabs/server/http/error"
	tbntime "github.com/turbinelabs/time"
)

var (
	emptyTimeSeries   = StatsTimeSeries{}
	emptyResponseErr  = httperr.New500("empty response", httperr.UnknownTransportCode)
	unexpectedDataErr = httperr.New500(
		"unexpected data beyond query response",
		httperr.UnknownTransportCode,
	)
)

const DefaultWavefrontServerUrl = "https://metrics.wavefront.com"

type wavefrontQueryBuilder struct {
	wavefrontServerUrl string
}

func newWavefrontQueryBuilder(wavefrontServerUrl string) (*wavefrontQueryBuilder, error) {
	_, err := url.ParseRequestURI(wavefrontServerUrl)
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(wavefrontServerUrl, "/") {
		wavefrontServerUrl = wavefrontServerUrl[0 : len(wavefrontServerUrl)-1]
	}

	return &wavefrontQueryBuilder{wavefrontServerUrl}, nil
}

// c.f. https://metrics.wavefront.com/api-docs/ui/#!/Query_APIs/chart
type wavefrontQueryResponse struct {
	TimeSeries []wavefrontTimeSeries `json:"timeseries"`
}

type wavefrontTimeSeries struct {
	Data []wavefrontPoint `json:"data"`
}

type wavefrontPoint [2]float64

type queryExpr interface {
	Format(api.OrgKey, string, *StatsQueryTimeSeries) string
}

type simpleQueryExpr struct{}

func (r *simpleQueryExpr) Format(
	orgKey api.OrgKey,
	zoneName string,
	q *StatsQueryTimeSeries,
) string {
	return formatQuery(
		formatMetric(orgKey, zoneName, q.DomainKey, q.RouteKey, q.Method, q.QueryType),
		q,
	)
}

type suffixedQueryExpr struct {
	queryType QueryType
	suffix    string
}

func (r *suffixedQueryExpr) Format(
	orgKey api.OrgKey,
	zoneName string,
	q *StatsQueryTimeSeries,
) string {
	metric := formatMetric(orgKey, zoneName, q.DomainKey, q.RouteKey, q.Method, r.queryType)
	if r.suffix != "" {
		metric = metric + "." + r.suffix
	}
	return formatQuery(metric, q)
}

type div []queryExpr

func (d div) Format(orgKey api.OrgKey, zoneName string, qts *StatsQueryTimeSeries) string {
	exprs := make([]string, len(d))
	for i, r := range d {
		exprs[i] = r.Format(orgKey, zoneName, qts)
	}
	return "(" + strings.Join(exprs, "/") + ")"
}

type sum []queryExpr

func (s sum) Format(orgKey api.OrgKey, zoneName string, qts *StatsQueryTimeSeries) string {
	exprs := make([]string, len(s))
	for i, r := range s {
		exprs[i] = r.Format(orgKey, zoneName, qts)
	}
	return "(" + strings.Join(exprs, "+") + ")"
}

var _ queryExpr = &simpleQueryExpr{}
var _ queryExpr = &suffixedQueryExpr{}
var _ queryExpr = div{}
var _ queryExpr = sum{}

var queryExprMap = map[QueryType]queryExpr{
	Requests:  &simpleQueryExpr{},
	Responses: &simpleQueryExpr{},
	SuccessfulResponses: sum{
		&suffixedQueryExpr{Responses, "1*"},
		&suffixedQueryExpr{Responses, "2*"},
		&suffixedQueryExpr{Responses, "3*"},
	},
	ErrorResponses:   &suffixedQueryExpr{Responses, "4*"},
	FailureResponses: &suffixedQueryExpr{Responses, "5*"},
	LatencyP50:       &simpleQueryExpr{},
	LatencyP99:       &simpleQueryExpr{},
	SuccessRate: div{
		sum{
			&suffixedQueryExpr{Responses, "1*"},
			&suffixedQueryExpr{Responses, "2*"},
			&suffixedQueryExpr{Responses, "3*"},
		},
		&suffixedQueryExpr{Requests, ""},
	},
}

func escape(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= '0' && r <= '9':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= 'a' && r <= 'z':
			return r
		case r == '_' || r == '-':
			return r
		default:
			return '_'
		}
	}, s)
}

// Given org and zone keys and a StatsQueryTimeSeries, produce a
// string containing the appropriate metric name.
func formatMetric(
	orgKey api.OrgKey,
	zoneName string,
	domainKey *api.DomainKey,
	routeKey *api.RouteKey,
	method *string,
	queryType QueryType,
) string {
	parts := []string{
		escape(string(orgKey)),
		escape(zoneName),
		"*",
		"*",
		"*",
		queryType.String(),
	}

	if domainKey != nil {
		parts[2] = escape(string(*domainKey))
	}

	if routeKey != nil {
		parts[3] = escape(string(*routeKey))
	}

	if method != nil {
		parts[4] = escape(string(*method))
	}

	return strings.Join(parts, ".")
}

// Given a metric name and a StatsQueryTimeSeries, produces a
// wavefront query with source tag filters for instances and/or
// clusters.
func formatQuery(metric string, qts *StatsQueryTimeSeries) string {
	instanceTagExprs := make([]string, len(qts.InstanceKeys))
	for idx, instanceKey := range qts.InstanceKeys {
		instanceTagExprs[idx] = fmt.Sprintf(`instance="%s"`, instanceKey)
	}
	tags := strings.Join(instanceTagExprs, " or ")

	if qts.ClusterKey != nil {
		if tags != "" {
			tags = fmt.Sprintf(
				`upstream="%s" and (%s)`,
				*qts.ClusterKey,
				tags,
			)
		} else {
			tags = fmt.Sprintf(`upstream="%s"`, *qts.ClusterKey)
		}
	}

	if tags != "" {
		return fmt.Sprintf(`ts("%s", %s)`, metric, tags)
	} else {
		return fmt.Sprintf(`ts("%s")`, metric)
	}
}

// Produces a wavefront charts API query URL.
func (builder wavefrontQueryBuilder) FormatWavefrontQueryUrl(
	startMicros int64,
	endMicros int64,
	orgKey api.OrgKey,
	zoneName string,
	qts *StatsQueryTimeSeries,
) string {
	startSeconds := tbntime.FromUnixMicro(startMicros).Unix()
	endSeconds := tbntime.FromUnixMicro(endMicros).Unix()

	expr := queryExprMap[qts.QueryType]
	query := expr.Format(orgKey, zoneName, qts)

	return fmt.Sprintf(
		"%s/chart/api?g=s&summarization=MEAN&s=%d&e=%d&q=%s",
		builder.wavefrontServerUrl,
		startSeconds,
		endSeconds,
		url.QueryEscape(query),
	)
}

// Decodes a wavefront response into a StatsTimeSeries object.
func decodeWavefrontResponse(response *http.Response) (StatsTimeSeries, *httperr.Error) {
	body := response.Body
	if body == nil {
		return emptyTimeSeries, emptyResponseErr
	}

	ts := wavefrontQueryResponse{}

	decoder := json.NewDecoder(body)
	if err := decoder.Decode(&ts); err != nil {
		return emptyTimeSeries, httperr.New500(err.Error(), httperr.UnknownTransportCode)
	}

	if decoder.More() {
		return emptyTimeSeries, unexpectedDataErr
	}

	resultTs := StatsTimeSeries{}
	if len(ts.TimeSeries) > 0 {
		wavefrontPoints := ts.TimeSeries[0].Data
		resultTs.Points = make([]StatsPoint, len(wavefrontPoints))
		for idx, wavefrontPoint := range wavefrontPoints {
			ts := time.Duration(wavefrontPoint[0])
			pointTs := int64(ts * time.Second / time.Microsecond)
			resultTs.Points[idx] = StatsPoint{
				Value:     wavefrontPoint[1],
				Timestamp: pointTs,
			}
		}
	}

	return resultTs, nil
}
