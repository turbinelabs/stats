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

// c.f. https://metrics.wavefront.com/api-docs/ui/#!/Query_APIs/chart
type wavefrontQueryResponse struct {
	TimeSeries []wavefrontTimeSeries `json:"timeseries"`
}

type wavefrontTimeSeries struct {
	Data []wavefrontPoint `json:"data"`
}

type wavefrontPoint [2]float64

// Given org and zone keys and a StatsQueryTimeSeries, produce a
// string containing the appropriate metric name.
func formatMetric(orgKey api.OrgKey, zoneKey api.ZoneKey, qts *StatsQueryTimeSeries) string {
	parts := []string{
		string(orgKey),
		string(zoneKey),
		"*",
		"*",
		"*",
		qts.QueryType.String(),
	}

	if qts.DomainKey != nil {
		parts[2] = string(*qts.DomainKey)
	}

	if qts.RouteKey != nil {
		parts[3] = string(*qts.RouteKey)
	}

	if qts.Method != nil {
		parts[4] = string(*qts.Method)
	}

	return strings.Join(parts, "/")
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
func formatWavefrontQueryUrl(
	startMicros int64,
	endMicros int64,
	orgKey api.OrgKey,
	zoneKey api.ZoneKey,
	qts *StatsQueryTimeSeries,
) string {
	startSeconds := tbntime.FromUnixMicro(startMicros).Unix()
	endSeconds := tbntime.FromUnixMicro(endMicros).Unix()

	metric := formatMetric(orgKey, zoneKey, qts)
	query := formatQuery(metric, qts)

	return fmt.Sprintf(
		"https://metrics.wavefront.com/chart/api?g=s&summarization=MEAN&s=%d&e=%d&q=%s",
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
