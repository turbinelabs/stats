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
	Query(api.OrgKey, string, *StatsQueryTimeSeries) string
	Metrics(api.OrgKey, string, *StatsQueryTimeSeries) []string
}

type simpleQueryExpr struct{}

func (e *simpleQueryExpr) Query(
	orgKey api.OrgKey,
	zoneName string,
	q *StatsQueryTimeSeries,
) string {
	return formatQuery(e.Metrics(orgKey, zoneName, q), q)
}

func (e *simpleQueryExpr) Metrics(
	orgKey api.OrgKey,
	zoneName string,
	q *StatsQueryTimeSeries,
) []string {
	return []string{
		formatMetric(orgKey, zoneName, q.DomainKey, q.RouteKey, q.Method, q.QueryType),
	}
}

type suffixedQueryExpr struct {
	queryType QueryType
	suffix    string
}

func (e *suffixedQueryExpr) Query(
	orgKey api.OrgKey,
	zoneName string,
	q *StatsQueryTimeSeries,
) string {
	return formatQuery(e.Metrics(orgKey, zoneName, q), q)
}

func (e *suffixedQueryExpr) Metrics(
	orgKey api.OrgKey,
	zoneName string,
	q *StatsQueryTimeSeries,
) []string {
	metric := formatMetric(orgKey, zoneName, q.DomainKey, q.RouteKey, q.Method, e.queryType)
	if e.suffix != "" {
		metric = metric + "." + e.suffix
	}
	return []string{metric}
}

type div []queryExpr

func (d div) Query(orgKey api.OrgKey, zoneName string, qts *StatsQueryTimeSeries) string {
	exprs := make([]string, len(d))
	for i, r := range d {
		exprs[i] = r.Query(orgKey, zoneName, qts)
	}
	return "(" + strings.Join(exprs, "/") + ")"
}

func (d div) Metrics(_ api.OrgKey, _ string, _ *StatsQueryTimeSeries) []string {
	return nil
}

type sum []queryExpr

func (s sum) Query(orgKey api.OrgKey, zoneName string, qts *StatsQueryTimeSeries) string {
	return formatQuery(s.Metrics(orgKey, zoneName, qts), qts)
}

func (s sum) Metrics(orgKey api.OrgKey, zoneName string, qts *StatsQueryTimeSeries) []string {
	metrics := make([]string, 0, len(s))
	for _, e := range s {
		metrics = append(metrics, e.Metrics(orgKey, zoneName, qts)...)
	}
	return metrics
}

type defaultExpr struct {
	value      float64
	underlying queryExpr
}

func (d *defaultExpr) Query(orgKey api.OrgKey, zoneName string, qts *StatsQueryTimeSeries) string {
	return fmt.Sprintf("default(%g, %s)", d.value, d.underlying.Query(orgKey, zoneName, qts))
}

func (d *defaultExpr) Metrics(
	orgKey api.OrgKey,
	zoneName string,
	qts *StatsQueryTimeSeries,
) []string {
	return d.underlying.Metrics(orgKey, zoneName, qts)
}

var _ queryExpr = &simpleQueryExpr{}
var _ queryExpr = &suffixedQueryExpr{}
var _ queryExpr = div{}
var _ queryExpr = sum{}
var _ queryExpr = &defaultExpr{}

var queryExprMap = map[QueryType]queryExpr{
	Requests: &defaultExpr{0.0, &simpleQueryExpr{}},
	Responses: &defaultExpr{
		0.0,
		sum{
			&suffixedQueryExpr{Responses, "*"},
		},
	},
	SuccessfulResponses: &defaultExpr{
		0.0,
		sum{
			&suffixedQueryExpr{Responses, "1*"},
			&suffixedQueryExpr{Responses, "2*"},
			&suffixedQueryExpr{Responses, "3*"},
		},
	},
	ErrorResponses:   &defaultExpr{0.0, &suffixedQueryExpr{Responses, "4*"}},
	FailureResponses: &defaultExpr{0.0, &suffixedQueryExpr{Responses, "5*"}},
	LatencyP50:       &defaultExpr{0.0, &simpleQueryExpr{}},
	LatencyP99:       &defaultExpr{0.0, &simpleQueryExpr{}},
	SuccessRate: &defaultExpr{
		0.0,
		div{
			sum{
				&suffixedQueryExpr{Responses, "1*"},
				&suffixedQueryExpr{Responses, "2*"},
				&suffixedQueryExpr{Responses, "3*"},
			},
			&suffixedQueryExpr{Requests, ""},
		},
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
func formatQuery(metrics []string, qts *StatsQueryTimeSeries) string {
	tagExprs := make([]string, 0, 4)

	if qts.RuleKey != nil {
		ruleTag := fmt.Sprintf(`rule="%s"`, *qts.RuleKey)
		tagExprs = append(tagExprs, ruleTag)
	}

	if qts.SharedRuleName != nil {
		sharedRuleTag := fmt.Sprintf(`shared_rule="%s"`, *qts.SharedRuleName)
		tagExprs = append(tagExprs, sharedRuleTag)
	}

	if qts.ClusterKey != nil {
		clusterTag := fmt.Sprintf(`upstream="%s"`, *qts.ClusterKey)
		tagExprs = append(tagExprs, clusterTag)
	}

	if len(qts.InstanceKeys) > 0 {
		instanceTagExprs := make([]string, len(qts.InstanceKeys))
		for idx, instanceKey := range qts.InstanceKeys {
			instanceTagExprs[idx] = fmt.Sprintf(`instance="%s"`, instanceKey)
		}

		instanceTag := strings.Join(instanceTagExprs, " or ")
		if len(instanceTagExprs) > 1 && len(tagExprs) > 0 {
			instanceTag = "(" + instanceTag + ")"
		}
		tagExprs = append(tagExprs, instanceTag)
	}

	// TODO: https://github.com/turbinelabs/tbn/issues/1399
	tags := strings.Join(tagExprs, " and ")
	if tags != "" {
		return fmt.Sprintf(`rawsum(ts("%s", %s))`, strings.Join(metrics, `" or "`), tags)
	} else {
		return fmt.Sprintf(`rawsum(ts("%s"))`, strings.Join(metrics, `" or "`))
	}
}

// Produces a wavefront charts API query URL.
func (builder wavefrontQueryBuilder) FormatWavefrontQueryUrl(
	startMicros int64,
	endMicros int64,
	granularity TimeGranularity,
	orgKey api.OrgKey,
	zoneName string,
	qts *StatsQueryTimeSeries,
) string {
	startSeconds := tbntime.FromUnixMicro(startMicros).Unix()
	endSeconds := tbntime.FromUnixMicro(endMicros).Unix()

	expr := queryExprMap[qts.QueryType]
	query := expr.Query(orgKey, zoneName, qts)

	var wavefrontGranularity string
	switch granularity {
	case Seconds:
		wavefrontGranularity = "s"
	case Minutes:
		wavefrontGranularity = "m"
	case Hours:
		wavefrontGranularity = "h"
	default:
		wavefrontGranularity = "s"
	}

	// TODO: https://github.com/turbinelabs/tbn/issues/1399
	return fmt.Sprintf(
		"%s/chart/api?strict=true&g=%s&summarization=SUM&s=%d&e=%d&q=%s",
		builder.wavefrontServerUrl,
		wavefrontGranularity,
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
