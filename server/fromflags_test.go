package server

import (
	"errors"
	"flag"
	"net/http"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/turbinelabs/cli/flags"
	"github.com/turbinelabs/server"
	"github.com/turbinelabs/server/cors"
	serverhandler "github.com/turbinelabs/server/handler"
	"github.com/turbinelabs/stats/server/handler"
	"github.com/turbinelabs/statsd"
	"github.com/turbinelabs/test/assert"
)

func TestNewFromFlags(t *testing.T) {
	flagset := flag.NewFlagSet("stats-server options", flag.PanicOnError)

	ff := NewFromFlags(flagset)
	assert.NonNil(t, ff)

	ffImpl := ff.(*fromFlags)
	assert.Nil(t, ffImpl.devMode.Strings)
	assert.Equal(t, ffImpl.wavefrontApiToken, "")
	assert.NonNil(t, ffImpl.ServerFromFlags)
	assert.NonNil(t, ffImpl.StatsFromFlags)
	assert.NonNil(t, ffImpl.AuthorizerFromFlags)
	assert.NonNil(t, ffImpl.MetricsCollectorFromFlags)
	assert.NonNil(t, ffImpl.CORSFromFlags)
}

func TestValidateServer(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockServerFromFlags := server.NewMockFromFlags(ctrl)
	mockServerFromFlags.EXPECT().Validate().Return(errors.New("boom"))

	ffImpl := &fromFlags{
		ServerFromFlags: mockServerFromFlags,
	}

	err := ffImpl.Validate()
	assert.ErrorContains(t, err, "boom")
}

func TestValidateMetricsCollector(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockServerFromFlags := server.NewMockFromFlags(ctrl)
	mockServerFromFlags.EXPECT().Validate().Return(nil)

	mockMetricsCollectorFromFlags := handler.NewMockMetricsCollectorFromFlags(ctrl)
	mockMetricsCollectorFromFlags.EXPECT().Validate().Return(errors.New("boom"))

	ffImpl := &fromFlags{
		ServerFromFlags:           mockServerFromFlags,
		MetricsCollectorFromFlags: mockMetricsCollectorFromFlags,
	}

	err := ffImpl.Validate()
	assert.ErrorContains(t, err, "boom")
}

func TestValidateWavefrontApiToken(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockServerFromFlags := server.NewMockFromFlags(ctrl)
	mockServerFromFlags.EXPECT().Validate().Return(nil)

	mockMetricsCollectorFromFlags := handler.NewMockMetricsCollectorFromFlags(ctrl)
	mockMetricsCollectorFromFlags.EXPECT().Validate().Return(nil)

	ffImpl := &fromFlags{
		ServerFromFlags:           mockServerFromFlags,
		MetricsCollectorFromFlags: mockMetricsCollectorFromFlags,
	}

	err := ffImpl.Validate()
	assert.ErrorContains(t, err, "--wavefront-api.token is a required flag")
}

func TestValidateSuccess(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockServerFromFlags := server.NewMockFromFlags(ctrl)
	mockServerFromFlags.EXPECT().Validate().Return(nil)

	mockMetricsCollectorFromFlags := handler.NewMockMetricsCollectorFromFlags(ctrl)
	mockMetricsCollectorFromFlags.EXPECT().Validate().Return(nil)

	ffImpl := &fromFlags{
		wavefrontApiToken:         "secret",
		ServerFromFlags:           mockServerFromFlags,
		MetricsCollectorFromFlags: mockMetricsCollectorFromFlags,
	}

	assert.Nil(t, ffImpl.Validate())
}

type makeTestCase struct {
	makeStatsError            error
	makeAuthError             error
	makeMetricsCollectorError error
	makeServerError           error
}

func (tc makeTestCase) run(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))

	serverFromFlags := server.NewMockFromFlags(ctrl)
	statsFromFlags := statsd.NewMockFromFlags(ctrl)
	authFromFlags := handler.NewMockAuthorizerFromFlags(ctrl)
	metricsCollectorFromFlags := handler.NewMockMetricsCollectorFromFlags(ctrl)
	corsFromFlags := cors.NewMockFromFlags(ctrl)

	ffImpl := &fromFlags{
		wavefrontServerUrl:        handler.DefaultWavefrontServerUrl,
		devMode:                   flags.NewStringsWithConstraint(),
		ServerFromFlags:           serverFromFlags,
		StatsFromFlags:            statsFromFlags,
		AuthorizerFromFlags:       authFromFlags,
		MetricsCollectorFromFlags: metricsCollectorFromFlags,
		CORSFromFlags:             corsFromFlags,
	}

	shouldFail := (tc.makeStatsError != nil ||
		tc.makeAuthError != nil ||
		tc.makeMetricsCollectorError != nil ||
		tc.makeServerError != nil)

	defer func() {
		statsServer, err := ffImpl.Make()
		if shouldFail {
			assert.Nil(t, statsServer)
			assert.NonNil(t, err)
		} else {
			assert.NonNil(t, statsServer)
			assert.Nil(t, err)
		}

		ctrl.Finish()
	}()

	if tc.makeStatsError != nil {
		statsFromFlags.EXPECT().Make().Return(nil, tc.makeStatsError)
		return
	}

	stats := statsd.NewMockStatsCloser(ctrl)
	statsFromFlags.EXPECT().Make().Return(stats, nil)

	if tc.makeAuthError != nil {
		authFromFlags.EXPECT().Make(gomock.Any()).Return(nil, tc.makeAuthError)
		return
	}

	auth := func(http.HandlerFunc) http.HandlerFunc {
		return nil
	}
	authFromFlags.EXPECT().Make(gomock.Any()).Return(auth, nil)

	if tc.makeMetricsCollectorError != nil {
		metricsCollectorFromFlags.EXPECT().
			Make(gomock.Any()).
			Return(nil, tc.makeMetricsCollectorError)
		return
	}

	metricsCollector := handler.NewMockMetricsCollector(ctrl)
	metricsCollectorFromFlags.EXPECT().Make(gomock.Any()).Return(metricsCollector, nil)

	stats.EXPECT().Scope("forward").Return(stats)
	stats.EXPECT().Scope("query").Return(stats)
	stats.EXPECT().Scope("cors").Return(stats)

	metricsCollector.EXPECT().AsHandler().Return(serverhandler.NotImplementedHandler)
	corsFromFlags.EXPECT().AllowedOrigins().Return([]string{"*"})
	if tc.makeServerError != nil {
		serverFromFlags.EXPECT().
			Make(gomock.Any(), gomock.Any(), stats, gomock.Any()).
			Return(nil, tc.makeServerError)
		return
	}

	server := server.NewMockServer(ctrl)
	serverFromFlags.EXPECT().
		Make(gomock.Any(), gomock.Any(), stats, gomock.Any()).
		Return(server, nil)

	server.EXPECT().DeferClose(metricsCollector)
}

func TestMakeStatsFailure(t *testing.T) {
	makeTestCase{
		makeStatsError: errors.New("stats creation failure"),
	}.run(t)
}

func TestMakeAuthorizerFailure(t *testing.T) {
	makeTestCase{
		makeAuthError: errors.New("authorizer creation failure"),
	}.run(t)
}

func TestMakeMetricsCollectorFailure(t *testing.T) {
	makeTestCase{
		makeMetricsCollectorError: errors.New("metrics collector creation failure"),
	}.run(t)

}

func TestMakeServerFailure(t *testing.T) {
	makeTestCase{
		makeServerError: errors.New("server creation failure"),
	}.run(t)
}

func TestMakeSuccess(t *testing.T) {
	makeTestCase{}.run(t)
}
