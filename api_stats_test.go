package stats

import (
	"errors"
	"log"
	"os"
	"testing"

	"github.com/golang/mock/gomock"

	apiflags "github.com/turbinelabs/api/client/flags"
	"github.com/turbinelabs/api/service/stats"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/test/assert"
)

func TestNewAPIStatsFromFlagsOptions(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	logger := log.New(os.Stderr, "test: ", 0)
	mockStatsClientFromFlags := apiflags.NewMockStatsClientFromFlags(ctrl)
	mockStatsClient := stats.NewMockStatsServiceV2(ctrl)
	mockZoneFromFlags := apiflags.NewMockZoneFromFlags(ctrl)

	fs := tbnflag.NewTestFlagSet().Scope("api", "")

	ff := newAPIStatsFromFlags(
		fs,
		SetStatsClientFromFlags(mockStatsClientFromFlags),
		SetZoneFromFlags(mockZoneFromFlags),
		SetLogger(logger),
	)

	ffImpl := ff.(*apiStatsFromFlags)
	assert.NonNil(t, ffImpl)

	assert.SameInstance(t, ffImpl.logger, logger)
	assert.SameInstance(t, ffImpl.statsClientFromFlags, mockStatsClientFromFlags)

	mockStatsClientFromFlags.EXPECT().APIKey().Return("")
	assert.ErrorContains(t, ff.Validate(), "--api.key must be specified")

	mockStatsClientFromFlags.EXPECT().APIKey().Return("key")
	mockZoneFromFlags.EXPECT().Name().Return("")
	assert.ErrorContains(t, ff.Validate(), "--api.zone-name must be specified")

	e := errors.New("boom")
	mockStatsClientFromFlags.EXPECT().APIKey().Return("key")
	mockZoneFromFlags.EXPECT().Name().Return("zone")
	mockStatsClientFromFlags.EXPECT().Validate().Return(e)
	assert.ErrorContains(t, ff.Validate(), "boom")

	mockStatsClientFromFlags.EXPECT().APIKey().Return("key")
	mockZoneFromFlags.EXPECT().Name().Return("zone")
	mockStatsClientFromFlags.EXPECT().Validate().Return(nil)
	assert.Nil(t, ff.Validate())

	mockStatsClientFromFlags.EXPECT().Make(logger).Return(nil, e)
	_, err := ff.Make()
	assert.ErrorContains(t, err, "boom")

	mockZoneFromFlags.EXPECT().Name().Return("zone")
	mockStatsClientFromFlags.EXPECT().Make(logger).Return(mockStatsClient, nil)
	s, err := ff.Make()
	assert.Nil(t, err)
	assert.NonNil(t, s)

	fs = tbnflag.NewTestFlagSet().Scope("api", "")
	ff = newAPIStatsFromFlags(
		fs,
		SetStatsClientFromFlags(mockStatsClientFromFlags),
	)

	ffImpl = ff.(*apiStatsFromFlags)
	assert.NonNil(t, ffImpl)
	mockStatsClientFromFlags.EXPECT().
		Make(gomock.Not(gomock.Nil())).
		Return(mockStatsClient, nil)
	s, err = ff.Make()
	assert.Nil(t, err)
	assert.NonNil(t, s)
}

func TestAPIStatsScope(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	underlying := NewMockStats(ctrl)
	sender := &apiSender{source: "unspecified", zone: "unspecified"}

	stats := &apiStats{underlying, sender}

	assert.SameInstance(t, stats.Scope("XYZ"), stats)
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
	sender := &apiSender{source: "unspecified", zone: "unspecified"}

	stats := &apiStats{underlying, sender}
	stats.AddTags(tagA, tagB)
	stats.AddTags(
		tagC,
		NewKVTag("proxy", "p"),
		NewKVTag("source", "s"),
		NewKVTag("zone", "z"),
		tagD,
	)

	assert.Equal(t, sender.proxy, "p")
	assert.Equal(t, sender.source, "s")
	assert.Equal(t, sender.zone, "z")
}

func TestNewLatchingAPIStats(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockStatsClient := stats.NewMockStatsServiceV2(ctrl)

	stats := NewLatchingAPIStats(
		mockStatsClient,
		DefaultLatchWindow,
		DefaultHistogramBaseValue,
		DefaultHistogramNumBuckets,
	)
	assert.NonNil(t, stats)

	statsImpl, ok := stats.(*apiStats)
	assert.True(t, ok)

	underlyingImpl, ok := statsImpl.Stats.(*xStats)
	assert.True(t, ok)

	wrappedSender, ok := underlyingImpl.sender.(*latchingSender)
	assert.True(t, ok)

	wrappedAPISender, ok := wrappedSender.underlying.(*apiSender)
	assert.True(t, ok)

	assert.SameInstance(t, statsImpl.apiSender, wrappedAPISender)
}
