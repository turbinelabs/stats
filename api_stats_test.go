package stats

import (
	"errors"
	"log"
	"os"
	"testing"

	"github.com/golang/mock/gomock"

	apiflags "github.com/turbinelabs/api/client/flags"
	"github.com/turbinelabs/api/service/stats"
	"github.com/turbinelabs/nonstdlib/executor"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/test/assert"
)

func TestNewAPIStatsFromFlagsOptions(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	logger := log.New(os.Stderr, "test: ", 0)
	mockExecFromFlags := executor.NewMockFromFlags(ctrl)
	mockExec := executor.NewMockExecutor(ctrl)
	mockStatsClientFromFlags := apiflags.NewMockStatsClientFromFlags(ctrl)
	mockStatsClient := stats.NewMockStatsService(ctrl)

	fs := tbnflag.NewTestFlagSet().Scope("api", "")

	ff := newAPIStatsFromFlags(
		fs,
		SetExecutorFromFlags(mockExecFromFlags),
		SetStatsClientFromFlags(mockStatsClientFromFlags),
		SetLogger(logger),
	)

	ffImpl := ff.(*apiStatsFromFlags)
	assert.NonNil(t, ffImpl)

	assert.SameInstance(t, ffImpl.logger, logger)
	assert.SameInstance(t, ffImpl.execFromFlags, mockExecFromFlags)
	assert.SameInstance(t, ffImpl.statsClientFromFlags, mockStatsClientFromFlags)

	mockStatsClientFromFlags.EXPECT().APIKey().Return("")
	assert.ErrorContains(t, ff.Validate(), "--api.key must be specified")

	e := errors.New("boom")
	mockStatsClientFromFlags.EXPECT().APIKey().Return("key")
	mockStatsClientFromFlags.EXPECT().Validate().Return(e)
	assert.ErrorContains(t, ff.Validate(), "boom")

	mockStatsClientFromFlags.EXPECT().APIKey().Return("key")
	mockStatsClientFromFlags.EXPECT().Validate().Return(nil)
	assert.Nil(t, ff.Validate())

	mockExecFromFlags.EXPECT().Make(logger).Return(mockExec)
	mockStatsClientFromFlags.EXPECT().Make(mockExec, logger).Return(nil, e)
	_, err := ff.Make(false)
	assert.ErrorContains(t, err, "boom")

	mockExecFromFlags.EXPECT().Make(logger).Return(mockExec)
	mockStatsClientFromFlags.EXPECT().Make(mockExec, logger).Return(mockStatsClient, nil)
	s, err := ff.Make(false)
	assert.Nil(t, err)
	assert.NonNil(t, s)

	fs = tbnflag.NewTestFlagSet().Scope("api", "")
	ff = newAPIStatsFromFlags(
		fs,
		SetExecutorFromFlags(mockExecFromFlags),
		SetStatsClientFromFlags(mockStatsClientFromFlags),
	)

	ffImpl = ff.(*apiStatsFromFlags)
	assert.NonNil(t, ffImpl)
	mockExecFromFlags.EXPECT().Make(gomock.Not(gomock.Nil())).Return(mockExec)
	mockStatsClientFromFlags.EXPECT().
		Make(mockExec, gomock.Not(gomock.Nil())).
		Return(mockStatsClient, nil)
	s, err = ff.Make(false)
	assert.Nil(t, err)
	assert.NonNil(t, s)
}

func TestAPIStatsAddTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	tagA := NewKVTag("a", "a")
	tagB := NewKVTag("b", "b")
	tagC := NewKVTag("c", "c")
	tagD := NewKVTag("d", "d")

	underlying := NewMockStats(ctrl)
	gomock.InOrder(
		underlying.EXPECT().AddTags(tagA),
		underlying.EXPECT().AddTags(tagB),
		underlying.EXPECT().AddTags(tagC),
		underlying.EXPECT().AddTags(tagD),
	)
	sender := &apiSender{source: "unspecified"}

	stats := &apiStats{underlying, sender}
	stats.AddTags(tagA, tagB)
	stats.AddTags(tagC, NewKVTag("source", "x"), tagD)

	assert.Equal(t, sender.source, "x")
}
