package route

import (
	"github.com/turbinelabs/server/cors"
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
	queryHandler handler.QueryHandler,
	allowedOrigins []string,
	allowedHeaders []string,
) serverroute.RouteSet {
	routes := serverroute.RouteSet{
		serverroute.NewAuthorized(
			stats,
			stats.Scope("forward"),
			serverroute.MethodPost,
			"/v1.0/stats/forward",
			authorizer,
			metricsCollector.AsHandler(),
		),
		serverroute.NewAuthorized(
			stats,
			stats.Scope("query"),
			serverroute.MethodGet,
			"/v1.0/stats/query",
			authorizer,
			queryHandler.AsHandler(),
		),
	}

	addCORSOrigin := serverhandler.CORSOriginAnnotator(allowedOrigins)

	for _, r := range routes {
		r.SetCORSFilter(addCORSOrigin)
	}

	routes = append(
		routes,
		cors.Route(stats, allowedOrigins, allowedHeaders).SetName("CorsHandler"),
	)

	return routes
}
