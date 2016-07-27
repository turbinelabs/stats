package route

import (
	serverhandler "github.com/turbinelabs/server/handler"
	serverroute "github.com/turbinelabs/server/route"
	"github.com/turbinelabs/stats/server/handler"
	"github.com/turbinelabs/statsd"
)

// Constructs routes for a stats server.
func MkRoutes(
	stats statsd.Stats,
	authorizer serverhandler.Authorizer,
	metricsCollector handler.MetricsCollector,
) serverroute.RouteSet {
	return serverroute.RouteSet{
		serverroute.NewAuthorized(
			stats,
			stats.Scope("forward"),
			serverroute.MethodPost,
			"/v1.0/metrics",
			authorizer,
			metricsCollector.AsHandler(),
		),
	}
}
