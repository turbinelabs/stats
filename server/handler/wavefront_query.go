package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/turbinelabs/api"
	httperr "github.com/turbinelabs/api/http/error"
	statsapi "github.com/turbinelabs/api/service/stats"
	"github.com/turbinelabs/api/service/stats/querytype"
	"github.com/turbinelabs/api/service/stats/timegranularity"
	tbntime "github.com/turbinelabs/nonstdlib/time"
)

var (
	emptyTimeSeries   = statsapi.TimeSeries{}
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

type queryContext struct {
	orgKey      api.OrgKey
	zoneName    string
	granularity timegranularity.TimeGranularity
	qts         *statsapi.QueryTimeSeries
}

type queryExpr interface {
	Query(*queryContext) string
	Metrics(*queryContext) []string
}

type simpleQueryExpr struct{}

func (e *simpleQueryExpr) Query(ctxt *queryContext) string {
	return formatQuery(e.Metrics(ctxt), ctxt.qts)
}

func (e *simpleQueryExpr) Metrics(ctxt *queryContext) []string {
	q := ctxt.qts
	return []string{
		formatMetric(
			ctxt.orgKey,
			ctxt.zoneName,
			q.DomainHost,
			q.RouteKey,
			q.Method,
			q.QueryType,
		),
	}
}

type suffixedQueryExpr struct {
	queryType querytype.QueryType
	suffix    string
}

func (e *suffixedQueryExpr) Query(ctxt *queryContext) string {
	return formatQuery(e.Metrics(ctxt), ctxt.qts)
}

func (e *suffixedQueryExpr) Metrics(ctxt *queryContext) []string {
	q := ctxt.qts
	metric := formatMetric(
		ctxt.orgKey,
		ctxt.zoneName,
		q.DomainHost,
		q.RouteKey,
		q.Method,
		e.queryType,
	)
	if e.suffix != "" {
		metric = metric + "." + e.suffix
	}
	return []string{metric}
}

type div []queryExpr

func (d div) Query(ctxt *queryContext) string {
	exprs := make([]string, len(d))
	for i, r := range d {
		exprs[i] = r.Query(ctxt)
	}
	return "(" + strings.Join(exprs, "/") + ")"
}

func (d div) Metrics(_ *queryContext) []string {
	return nil
}

type sum struct {
	underlying queryExpr
}

func (s sum) Query(ctxt *queryContext) string {
	return fmt.Sprintf(`rawsum(%s)`, s.underlying.Query(ctxt))
}

func (s sum) Metrics(ctxt *queryContext) []string {
	return s.underlying.Metrics(ctxt)
}

type or []queryExpr

func (o or) Query(ctxt *queryContext) string {
	return formatQuery(o.Metrics(ctxt), ctxt.qts)
}

func (o or) Metrics(ctxt *queryContext) []string {
	metrics := make([]string, 0, len(o))
	for _, e := range o {
		metrics = append(metrics, e.Metrics(ctxt)...)
	}
	return metrics
}

type defaultExpr struct {
	value      float64
	underlying queryExpr
}

func (d *defaultExpr) Query(ctxt *queryContext) string {
	return fmt.Sprintf("default(%g, %s)", d.value, d.underlying.Query(ctxt))
}

func (d *defaultExpr) Metrics(ctxt *queryContext) []string {
	return d.underlying.Metrics(ctxt)
}

type percentileExpr struct {
	percentile float64
	underlying queryExpr
}

func (p *percentileExpr) Query(ctxt *queryContext) string {
	return fmt.Sprintf("percentile(%g, %s)", p.percentile, p.underlying.Query(ctxt))
}

func (p *percentileExpr) Metrics(ctxt *queryContext) []string {
	return p.underlying.Metrics(ctxt)
}

type alignExpr struct {
	aggregation string
	underlying  queryExpr
}

func (a *alignExpr) Query(ctxt *queryContext) string {
	windowUnit := granularityToUnit(ctxt.granularity)

	return fmt.Sprintf(
		"align(1%s, %s, %s)",
		windowUnit,
		a.aggregation,
		a.underlying.Query(ctxt),
	)
}

func (a *alignExpr) Metrics(ctxt *queryContext) []string {
	return a.underlying.Metrics(ctxt)
}

var _ queryExpr = &simpleQueryExpr{}
var _ queryExpr = &suffixedQueryExpr{}
var _ queryExpr = div{}
var _ queryExpr = or{}
var _ queryExpr = sum{}
var _ queryExpr = &defaultExpr{}
var _ queryExpr = &percentileExpr{}
var _ queryExpr = &alignExpr{}

var queryExprMap = map[querytype.QueryType]queryExpr{
	querytype.Requests:  &defaultExpr{0.0, &sum{&alignExpr{"sum", &simpleQueryExpr{}}}},
	querytype.Responses: &defaultExpr{0.0, &sum{&alignExpr{"sum", &suffixedQueryExpr{querytype.Responses, "*"}}}},
	querytype.SuccessfulResponses: &defaultExpr{
		0.0,
		&sum{
			&alignExpr{
				"sum",
				or{
					&suffixedQueryExpr{querytype.Responses, "1*"},
					&suffixedQueryExpr{querytype.Responses, "2*"},
					&suffixedQueryExpr{querytype.Responses, "3*"},
				},
			},
		},
	},
	querytype.ErrorResponses: &defaultExpr{
		0.0,
		&sum{
			&alignExpr{
				"sum",
				&suffixedQueryExpr{querytype.Responses, "4*"},
			},
		},
	},
	querytype.FailureResponses: &defaultExpr{
		0.0,
		&sum{
			&alignExpr{
				"sum",
				&suffixedQueryExpr{querytype.Responses, "5*"},
			},
		},
	},
	querytype.LatencyP50: &defaultExpr{
		0.0,
		&percentileExpr{
			50.0,
			&alignExpr{
				"mean",
				&simpleQueryExpr{},
			},
		},
	},
	querytype.LatencyP99: &defaultExpr{
		0.0,
		&percentileExpr{
			99.0,
			&alignExpr{
				"mean",
				&simpleQueryExpr{},
			},
		},
	},
	querytype.SuccessRate: &defaultExpr{
		1.0,
		div{
			sum{
				&alignExpr{
					"sum",
					or{
						&suffixedQueryExpr{querytype.Responses, "1*"},
						&suffixedQueryExpr{querytype.Responses, "2*"},
						&suffixedQueryExpr{querytype.Responses, "3*"},
					},
				},
			},
			sum{
				&alignExpr{
					"sum",
					&suffixedQueryExpr{querytype.Requests, ""},
				},
			},
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

// Given org and zone keys and a QueryTimeSeries, produce a
// string containing the appropriate metric name.
func formatMetric(
	orgKey api.OrgKey,
	zoneName string,
	domainHost *string,
	routeKey *api.RouteKey,
	method *string,
	queryType querytype.QueryType,
) string {
	metricName := queryType.String()
	if queryType == querytype.LatencyP50 || queryType == querytype.LatencyP99 {
		metricName = "latency"
	}

	parts := []string{
		escape(string(orgKey)),
		escape(zoneName),
		"*",
		"*",
		"*",
		metricName,
	}

	if domainHost != nil {
		parts[2] = escape(*domainHost)
		if strings.IndexRune(*domainHost, ':') == -1 {
			parts[2] += "_*"
		}
	}

	if routeKey != nil {
		parts[3] = escape(string(*routeKey))
	}

	if method != nil {
		parts[4] = escape(string(*method))
	}

	return strings.Join(parts, ".")
}

// Given a metric name and a QueryTimeSeries, produces a
// wavefront query with source tag filters for instances and/or
// clusters.
func formatQuery(metrics []string, qts *statsapi.QueryTimeSeries) string {
	tagExprs := make([]string, 0, 4)

	if qts.RuleKey != nil {
		ruleTag := fmt.Sprintf(`rule="%s"`, *qts.RuleKey)
		tagExprs = append(tagExprs, ruleTag)
	}

	if qts.SharedRuleName != nil {
		sharedRuleTag := fmt.Sprintf(`shared_rule="%s"`, *qts.SharedRuleName)
		tagExprs = append(tagExprs, sharedRuleTag)
	}

	if qts.ClusterName != nil {
		clusterTag := fmt.Sprintf(`upstream="%s"`, *qts.ClusterName)
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

	tags := strings.Join(tagExprs, " and ")
	if tags != "" {
		return fmt.Sprintf(`ts("%s", %s)`, strings.Join(metrics, `" or "`), tags)
	} else {
		return fmt.Sprintf(`ts("%s")`, strings.Join(metrics, `" or "`))
	}
}

func granularityToUnit(g timegranularity.TimeGranularity) string {
	switch g {
	case timegranularity.Seconds:
		return "s"
	case timegranularity.Minutes:
		return "m"
	case timegranularity.Hours:
		return "h"
	default:
		return "s"
	}
}

// Produces a wavefront charts API query URL.
func (builder wavefrontQueryBuilder) FormatWavefrontQueryUrl(
	startMicros int64,
	endMicros int64,
	granularity timegranularity.TimeGranularity,
	orgKey api.OrgKey,
	zoneName string,
	qts *statsapi.QueryTimeSeries,
) string {
	startSeconds := tbntime.FromUnixMicro(startMicros).Unix()
	endSeconds := tbntime.FromUnixMicro(endMicros).Unix()

	expr := queryExprMap[qts.QueryType]

	ctxt := &queryContext{
		orgKey:      orgKey,
		zoneName:    zoneName,
		granularity: granularity,
		qts:         qts,
	}
	query := expr.Query(ctxt)

	wavefrontGranularity := granularityToUnit(granularity)

	var summarization string
	switch qts.QueryType {
	case querytype.LatencyP50, querytype.LatencyP99:
		summarization = "MEAN"
	default:
		summarization = "SUM"
	}

	return fmt.Sprintf(
		"%s/chart/api?strict=true&g=%s&summarization=%s&s=%d&e=%d&q=%s",
		builder.wavefrontServerUrl,
		wavefrontGranularity,
		summarization,
		startSeconds,
		endSeconds,
		url.QueryEscape(query),
	)
}

// Decodes a wavefront response into a TimeSeries object.
func decodeWavefrontResponse(response *http.Response) (statsapi.TimeSeries, *httperr.Error) {
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

	resultTs := statsapi.TimeSeries{}
	if len(ts.TimeSeries) > 0 {
		wavefrontPoints := ts.TimeSeries[0].Data
		resultTs.Points = make([]statsapi.Point, len(wavefrontPoints))
		for idx, wavefrontPoint := range wavefrontPoints {
			ts := time.Duration(wavefrontPoint[0])
			pointTs := int64(ts * time.Second / time.Microsecond)
			resultTs.Points[idx] = statsapi.Point{
				Value:     wavefrontPoint[1],
				Timestamp: pointTs,
			}
		}
	}

	return resultTs, nil
}
