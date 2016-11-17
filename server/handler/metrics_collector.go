package handler

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/turbinelabs/api"
	apihttp "github.com/turbinelabs/api/http"
	httperr "github.com/turbinelabs/api/http/error"
	"github.com/turbinelabs/logparser/forwarder"
	"github.com/turbinelabs/logparser/metric"
	"github.com/turbinelabs/nonstdlib/ptr"
	"github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/server"
	"github.com/turbinelabs/stats"
	"github.com/turbinelabs/stats/server/handler/requestcontext"
)

// MetricsCollector abstracts the collection of metrics and their
// subsequent delivery to an external metrics storage service.
type MetricsCollector interface {
	// Forwards the given statistics payload to an external
	// service. Returns the number of metrics accepted by the
	// external service and the first error encountered. Errors
	// may occur while encoding metrics for forwarding or during
	// forwarding itself.
	Forward(api.OrgKey, *stats.StatsPayload) (int, error)

	// Closes any resources associated with the external service.
	Close() error

	// Returns an http.HandlerFunc that accepts statistics
	// payloads in JSON format and invokes Forward.
	AsHandler() http.HandlerFunc
}

// Constructs a new MetricsCollector that forwards stats using the
// given Forwarder.
func NewMetricsCollector(fwd forwarder.Forwarder) MetricsCollector {
	return &metricsCollector{fwd}
}

type metricsCollector struct {
	forwarder forwarder.Forwarder
}

var _ server.Closer = &metricsCollector{}

func (f *metricsCollector) Forward(orgKey api.OrgKey, payload *stats.StatsPayload) (int, error) {
	source, err := metric.NewSource(payload.Source, "")
	if err != nil {
		return 0, err
	}

	var firstErr error
	rememberFirstError := func(e error) {
		if firstErr == nil {
			firstErr = e
		}
	}

	values := make([]metric.MetricValue, 0, len(payload.Stats))
	for _, stat := range payload.Stats {
		parts := strings.Split(stat.Name, "/")
		for idx, part := range parts {
			parts[idx] = escape(part)
		}
		cleanName := strings.Join(parts, ".")
		m, err := source.NewMetric(string(orgKey) + "." + cleanName)
		if err != nil {
			rememberFirstError(err)
			continue
		}

		values = append(
			values,
			metric.MetricValue{
				Metric:    m,
				Value:     stat.Value,
				Timestamp: ptr.Time(time.FromUnixMicro(stat.Timestamp)),
				Tags:      stat.Tags,
			},
		)
	}

	if len(values) == 0 {
		return 0, firstErr
	}

	sent, err := f.forwarder.Send(values)
	rememberFirstError(err)

	return sent, firstErr
}

func asHandler(f MetricsCollector) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		rrw := apihttp.RichResponseWriter{rw}

		requestContext := requestcontext.New(r)
		if orgKey, ok := requestContext.GetOrgKey(); ok {
			fr := metricsCollectorRequest{r}

			payload, err := fr.getPayload()
			if err != nil {
				rrw.WriteEnvelope(err, nil)
				return
			}

			num, err := f.Forward(orgKey, payload)

			rrw.WriteEnvelope(err, &stats.Result{NumAccepted: num})
		} else {
			rrw.WriteEnvelope(
				httperr.New500("authorization config error", httperr.MiscErrorCode),
				nil,
			)
		}
	}
}

func (f *metricsCollector) Close() error {
	return f.forwarder.Close()
}

func (f *metricsCollector) AsHandler() http.HandlerFunc {
	return asHandler(f)
}

// An http.Request wrapper that encapsulates conversion of the request
// Body into a stats.StatsPayload.
type metricsCollectorRequest struct {
	*http.Request
}

func (f *metricsCollectorRequest) getPayload() (*stats.StatsPayload, error) {
	body := f.Request.Body
	if body == nil {
		return nil, httperr.New400("no body available", httperr.UnknownNoBodyCode)
	}

	b, err := ioutil.ReadAll(body)
	defer body.Close()
	if err != nil {
		return nil,
			httperr.New500("could not read request body", httperr.UnknownTransportCode)
	}

	stats := &stats.StatsPayload{}
	err = json.Unmarshal(b, stats)
	if err != nil {
		return nil,
			httperr.New400(
				"error handling JSON content: "+string(b),
				httperr.UnknownDecodingCode,
			)
	}

	return stats, nil
}
