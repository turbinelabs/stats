package client

import (
	"errors"
	"flag"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/turbinelabs/api/client/flags"
	apihttp "github.com/turbinelabs/api/http"
	"github.com/turbinelabs/nonstdlib/executor"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/test/assert"
	"github.com/turbinelabs/test/log"
)

func TestFromFlagsValidatesNormalClient(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	fs := flag.NewFlagSet("stats-client", flag.PanicOnError)
	pfs := tbnflag.NewPrefixedFlagSet(fs, "pfix", "")

	apiConfigFromFlags := flags.NewMockAPIConfigFromFlags(ctrl)

	ff := NewFromFlags(pfs, WithAPIConfigFromFlags(apiConfigFromFlags))
	assert.NonNil(t, ff)
	assert.SameInstance(t, apiConfigFromFlags, ff.(*fromFlags).apiConfigFromFlags)

	apiConfigFromFlags.EXPECT().Validate().Return(nil)
	assert.Nil(t, ff.Validate())

	expectedErr := errors.New("boom")
	apiConfigFromFlags.EXPECT().Validate().Return(expectedErr)
	assert.Equal(t, ff.Validate(), expectedErr)
}

func TestFromFlagsDelegatesToAPIConfigFromFlags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	fs := flag.NewFlagSet("stats-client", flag.PanicOnError)
	pfs := tbnflag.NewPrefixedFlagSet(fs, "pfix", "")

	mockExec := executor.NewMockExecutor(ctrl)

	endpoint, err := apihttp.NewEndpoint(apihttp.HTTPS, "example.com", 538)
	assert.Nil(t, err)
	assert.NonNil(t, endpoint)

	apiConfigFromFlags := flags.NewMockAPIConfigFromFlags(ctrl)
	apiConfigFromFlags.EXPECT().MakeEndpoint().Return(endpoint, nil)
	apiConfigFromFlags.EXPECT().APIKey().Return("OTAY")

	ff := NewFromFlags(pfs, WithAPIConfigFromFlags(apiConfigFromFlags))
	assert.NonNil(t, ff)

	fs.Parse([]string{
		"-pfix.batch=false",
	})

	ffImpl := ff.(*fromFlags)
	assert.False(t, ffImpl.useBatching)
	assert.Equal(t, ffImpl.maxBatchDelay, DefaultMaxBatchDelay)
	assert.Equal(t, ffImpl.maxBatchSize, DefaultMaxBatchSize)

	statsClient, err := ff.Make(mockExec, log.NewNoopLogger())
	assert.NonNil(t, statsClient)
	assert.Nil(t, err)

	statsClientImpl, ok := statsClient.(*httpStatsV1)
	assert.True(t, ok)
	assert.SameInstance(t, statsClientImpl.exec, mockExec)

	expectedErr := errors.New("no endpoints for you!")
	apiConfigFromFlags.EXPECT().
		MakeEndpoint().
		Return(apihttp.Endpoint{}, expectedErr)

	fs = flag.NewFlagSet("stats-client", flag.PanicOnError)
	pfs = tbnflag.NewPrefixedFlagSet(fs, "pfix", "")
	ff = NewFromFlags(pfs, WithAPIConfigFromFlags(apiConfigFromFlags))
	assert.NonNil(t, ff)

	statsClient, err = ff.Make(mockExec, log.NewNoopLogger())
	assert.Nil(t, statsClient)
	assert.NonNil(t, err)
}

func TestFromFlagsCachesClient(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	fs := flag.NewFlagSet("stats-client", flag.PanicOnError)
	pfs := tbnflag.NewPrefixedFlagSet(fs, "pfix", "")

	mockExec := executor.NewMockExecutor(ctrl)
	otherMockExec := executor.NewMockExecutor(ctrl)

	endpoint, err := apihttp.NewEndpoint(apihttp.HTTPS, "example.com", 538)
	assert.Nil(t, err)
	assert.NonNil(t, endpoint)

	apiConfigFromFlags := flags.NewMockAPIConfigFromFlags(ctrl)
	apiConfigFromFlags.EXPECT().MakeEndpoint().Return(endpoint, nil)
	apiConfigFromFlags.EXPECT().APIKey().Return("OTAY")

	ff := NewFromFlags(pfs, WithAPIConfigFromFlags(apiConfigFromFlags))
	assert.NonNil(t, ff)

	statsClient, err := ff.Make(mockExec, log.NewNoopLogger())
	assert.NonNil(t, statsClient)
	assert.Nil(t, err)

	statsClient2, err := ff.Make(otherMockExec, log.NewNoopLogger())
	assert.NonNil(t, statsClient2)
	assert.Nil(t, err)

	assert.SameInstance(t, statsClient2, statsClient)
}

func TestFromFlagsCreatesBatchingClient(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	fs := flag.NewFlagSet("stats-client", flag.PanicOnError)
	pfs := tbnflag.NewPrefixedFlagSet(fs, "pfix", "")

	mockExec := executor.NewMockExecutor(ctrl)

	endpoint, err := apihttp.NewEndpoint(apihttp.HTTPS, "example.com", 538)
	assert.Nil(t, err)
	assert.NonNil(t, endpoint)

	apiConfigFromFlags := flags.NewMockAPIConfigFromFlags(ctrl)
	apiConfigFromFlags.EXPECT().MakeEndpoint().Return(endpoint, nil)
	apiConfigFromFlags.EXPECT().APIKey().Return("OTAY")

	ff := NewFromFlags(pfs, WithAPIConfigFromFlags(apiConfigFromFlags))
	assert.NonNil(t, ff)

	fs.Parse([]string{
		"-pfix.max-batch-delay=5s",
		"-pfix.max-batch-size=99",
	})

	ffImpl := ff.(*fromFlags)
	assert.True(t, ffImpl.useBatching)
	assert.Equal(t, ffImpl.maxBatchDelay, 5*time.Second)
	assert.Equal(t, ffImpl.maxBatchSize, 99)

	statsClient, err := ff.Make(mockExec, log.NewNoopLogger())
	assert.NonNil(t, statsClient)
	assert.Nil(t, err)

	statsClientImpl, ok := statsClient.(*httpBatchingStatsV1)
	assert.True(t, ok)

	underlyingStatsClientImpl, ok := statsClientImpl.internalStatsClient.(*httpStatsV1)
	assert.True(t, ok)
	assert.SameInstance(t, underlyingStatsClientImpl.exec, mockExec)
}

func TestFromFlagsValidatesBatchingClient(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	fs := flag.NewFlagSet("stats-client", flag.PanicOnError)
	pfs := tbnflag.NewPrefixedFlagSet(fs, "pfix", "")

	apiConfigFromFlags := flags.NewMockAPIConfigFromFlags(ctrl)

	ff := NewFromFlags(pfs, WithAPIConfigFromFlags(apiConfigFromFlags))
	assert.NonNil(t, ff)

	fs.Parse([]string{
		"-pfix.batch=true",
		"-pfix.max-batch-delay=0s",
	})

	assert.ErrorContains(t, ff.Validate(), "max-batch-delay")

	fs.Parse([]string{
		"-pfix.batch=true",
		"-pfix.max-batch-delay=1s",
		"-pfix.max-batch-size=0",
	})

	assert.ErrorContains(t, ff.Validate(), "max-batch-size")

	fs.Parse([]string{
		"-pfix.batch=true",
		"-pfix.max-batch-delay=1s",
		"-pfix.max-batch-size=1",
	})

	expectedErr := errors.New("boom")
	apiConfigFromFlags.EXPECT().Validate().Return(expectedErr)
	assert.Equal(t, ff.Validate(), expectedErr)

	apiConfigFromFlags.EXPECT().Validate().Return(nil)
	assert.Nil(t, ff.Validate())
}
