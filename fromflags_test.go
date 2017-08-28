package stats

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	apiflags "github.com/turbinelabs/api/client/flags"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/test/assert"
)

type validateTestCase struct {
	args                []string
	expectErrorContains string
}

func (vtc *validateTestCase) check(t *testing.T) {
	desc := strings.Join(vtc.args, " ")
	assert.Group(desc, t, func(g *assert.G) {
		fs := tbnflag.NewTestFlagSet()
		ff := NewFromFlags(fs, EnableAPIStatsBackend())
		err := fs.Parse(vtc.args)
		assert.Nil(g, err)
		if vtc.expectErrorContains != "" {
			assert.ErrorContains(g, ff.Validate(), vtc.expectErrorContains)
		} else {
			assert.Nil(g, ff.Validate())
		}
	})
}

func TestFromFlagsParse(t *testing.T) {
	fs := tbnflag.NewTestFlagSet()
	ff := NewFromFlags(fs)
	err := fs.Parse([]string{
		"--backends=dogstatsd,statsd",
		"--dogstatsd.host=localhost",
		"--dogstatsd.port=8000",
		"--dogstatsd.max-packet-len=512",
		"--statsd.host=remotehost",
		"--statsd.port=9000",
		"--statsd.flush-interval=30s",
	})
	assert.Nil(t, err)

	ffImpl := ff.(*fromFlags)

	dsdFromFlags, ok := ffImpl.statsFromFlagses[dogstatsdName]
	assert.True(t, ok)
	dsdFromFlagsImpl := dsdFromFlags.(*dogstatsdFromFlags)
	assert.Equal(t, dsdFromFlagsImpl.flagScope, "dogstatsd.")
	assert.Equal(t, dsdFromFlagsImpl.host, "localhost")
	assert.Equal(t, dsdFromFlagsImpl.port, 8000)
	assert.Equal(t, dsdFromFlagsImpl.maxPacketLen, 512)
	assert.Equal(t, dsdFromFlagsImpl.flushInterval, defaultFlushInterval)

	sdFromFlags, ok := ffImpl.statsFromFlagses[statsdName]
	assert.True(t, ok)
	sdFromFlagsImpl := sdFromFlags.(*statsdFromFlags)
	assert.Equal(t, sdFromFlagsImpl.flagScope, "statsd.")
	assert.Equal(t, sdFromFlagsImpl.host, "remotehost")
	assert.Equal(t, sdFromFlagsImpl.port, 9000)
	assert.Equal(t, sdFromFlagsImpl.maxPacketLen, defaultMaxPacketLen)
	assert.Equal(t, sdFromFlagsImpl.flushInterval, 30*time.Second)
}

func TestFromFlagsOptions(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockStatsClientFromFlags := apiflags.NewMockStatsClientFromFlags(ctrl)
	mockStatsClientFromFlags.EXPECT().APIKey().Return("key")
	mockStatsClientFromFlags.EXPECT().Validate().Return(errors.New("passed thru"))

	mockZoneKeyFromFlags := apiflags.NewMockZoneKeyFromFlags(ctrl)
	mockZoneKeyFromFlags.EXPECT().ZoneName().Return("zone")

	fs := tbnflag.NewTestFlagSet()

	ff := NewFromFlags(
		fs,
		EnableAPIStatsBackend(),
		APIStatsOptions(
			SetStatsClientFromFlags(mockStatsClientFromFlags),
			SetZoneKeyFromFlags(mockZoneKeyFromFlags),
		),
	)
	ffImpl := ff.(*fromFlags)
	assert.ArrayEqual(t, ffImpl.backends.Strings, []string{})

	err := fs.Parse([]string{
		"--backends=api",
	})
	assert.Nil(t, err)
	assert.ErrorContains(t, ff.Validate(), "passed thru")

	fs = tbnflag.NewTestFlagSet()
	ff = NewFromFlags(
		fs,
		DefaultBackends("statsd", "wavefront"),
	)
	ffImpl = ff.(*fromFlags)
	assert.ArrayEqual(t, ffImpl.backends.Strings, []string{"statsd", "wavefront"})

	fs = tbnflag.NewTestFlagSet()
	ff = NewFromFlags(
		fs,
		DefaultBackends("api", "DOGSTATSD", "fred"),
	)
	ffImpl = ff.(*fromFlags)
	assert.ArrayEqual(t, ffImpl.backends.Strings, []string{"dogstatsd"})
}

func TestFromFlagsValidate(t *testing.T) {
	testCases := []validateTestCase{
		// no backends
		{
			[]string{},
			"no backends specified",
		},
		// dogstatsd
		{
			[]string{
				"--backends=dogstatsd",
				"--dogstatsd.host=nope:nope",
			},
			"--dogstatsd.host or --dogstatsd.port is invalid",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--dogstatsd.flush-interval=0",
			},
			"--dogstatsd.flush-interval must be greater than zero",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--dogstatsd.latch=true",
			},
			"",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--dogstatsd.latch=true",
				"--dogstatsd.latch.window=0",
			},
			"--dogstatsd.latch.window must be greater than 0",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--dogstatsd.latch=true",
				"--dogstatsd.latch.base-value=0",
			},
			"--dogstatsd.latch.base-value must be greater than 0",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--dogstatsd.latch=true",
				"--dogstatsd.latch.buckets=1",
			},
			"--dogstatsd.latch.buckets must be greater than 1",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--dogstatsd.latch=true",
				"--dogstatsd.latch.window=1m",
				"--dogstatsd.latch.base-value=1",
				"--dogstatsd.latch.buckets=8",
			},
			"",
		},
		// prometheus
		{
			[]string{
				"--backends=prometheus",
				"--prometheus.addr=nope",
			},
			"--prometheus.addr is invalid",
		},
		{
			[]string{
				"--backends=prometheus",
				"--prometheus.addr=127.0.0.1:9000",
			},
			"",
		},
		// statsd
		{
			[]string{
				"--backends=statsd",
				"--statsd.host=nope:nope",
			},
			"--statsd.host or --statsd.port is invalid",
		},
		{
			[]string{
				"--backends=statsd",
				"--statsd.flush-interval=0",
			},
			"--statsd.flush-interval must be greater than zero",
		},
		{
			[]string{
				"--backends=statsd",
				"--statsd.flush-interval=1s",
			},
			"",
		},
		{
			[]string{
				"--backends=statsd",
				"--statsd.latch=true",
			},
			"",
		},
		{
			[]string{
				"--backends=statsd",
				"--statsd.latch=true",
				"--statsd.latch.window=0",
			},
			"--statsd.latch.window must be greater than 0",
		},
		{
			[]string{
				"--backends=statsd",
				"--statsd.latch=true",
				"--statsd.latch.base-value=0",
			},
			"--statsd.latch.base-value must be greater than 0",
		},
		{
			[]string{
				"--backends=statsd",
				"--statsd.latch=true",
				"--statsd.latch.buckets=1",
			},
			"--statsd.latch.buckets must be greater than 1",
		},
		{
			[]string{
				"--backends=statsd",
				"--statsd.latch=true",
				"--statsd.latch.window=1m",
				"--statsd.latch.base-value=1",
				"--statsd.latch.buckets=8",
			},
			"",
		},
		// wavefront
		{
			[]string{
				"--backends=wavefront",
				"--wavefront.host=nope:nope",
			},
			"--wavefront.host or --wavefront.port is invalid",
		},
		{
			[]string{
				"--backends=wavefront",
				"--wavefront.flush-interval=0",
			},
			"--wavefront.flush-interval must be greater than zero",
		},
		{
			[]string{
				"--backends=wavefront",
				"--wavefront.flush-interval=1s",
			},
			"",
		},
		{
			[]string{
				"--backends=wavefront",
				"--wavefront.latch=true",
			},
			"",
		},
		{
			[]string{
				"--backends=wavefront",
				"--wavefront.latch=true",
				"--wavefront.latch.window=0",
			},
			"--wavefront.latch.window must be greater than 0",
		},
		{
			[]string{
				"--backends=wavefront",
				"--wavefront.latch=true",
				"--wavefront.latch.base-value=0",
			},
			"--wavefront.latch.base-value must be greater than 0",
		},
		{
			[]string{
				"--backends=wavefront",
				"--wavefront.latch=true",
				"--wavefront.latch.buckets=1",
			},
			"--wavefront.latch.buckets must be greater than 1",
		},
		{
			[]string{
				"--backends=wavefront",
				"--wavefront.latch=true",
				"--wavefront.latch.window=1m",
				"--wavefront.latch.base-value=1",
				"--wavefront.latch.buckets=8",
			},
			"",
		},
		// api
		{
			[]string{
				"--backends=api",
			},
			"--api.key must be specified",
		},
		{
			[]string{
				"--backends=api",
				"--api.key=keyzor",
			},
			"--api.zone-name must be specified",
		},
		{
			[]string{
				"--backends=api",
				"--api.key=keyzor",
				"--api.zone-name=zoner",
			},
			"",
		},
		{
			[]string{
				"--backends=api",
				"--api.key=keyzor",
				"--api.zone-name=zoner",
				"--api.latch=true",
			},
			"",
		},
		{
			[]string{
				"--backends=api",
				"--api.key=keyzor",
				"--api.zone-name=zoner",
				"--api.latch=true",
				"--api.latch.window=0",
			},
			"--api.latch.window must be greater than 0",
		},
		{
			[]string{
				"--backends=api",
				"--api.key=keyzor",
				"--api.zone-name=zoner",
				"--api.latch=true",
				"--api.latch.base-value=0",
			},
			"--api.latch.base-value must be greater than 0",
		},
		{
			[]string{
				"--backends=api",
				"--api.key=keyzor",
				"--api.zone-name=zoner",
				"--api.latch=true",
				"--api.latch.buckets=1",
			},
			"--api.latch.buckets must be greater than 1",
		},
		{
			[]string{
				"--backends=api",
				"--api.key=keyzor",
				"--api.zone-name=zoner",
				"--api.latch=true",
				"--api.latch.window=1m",
				"--api.latch.base-value=1",
				"--api.latch.buckets=8",
			},
			"",
		},

		// node, source, and tags
		{
			[]string{
				"--backends=dogstatsd",
				"--node=xyz",
				"--source=pdq",
				"--tags=other",
			},
			"",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--node=xyz",
				"--tags=node=notxyz",
			},
			"cannot combine --tags=node=... and --node",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--tags=node=xyz,node=notxyz",
			},
			"cannot specify multiple tags named node",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--source=xyz",
				"--tags=source=notxyz",
			},
			"cannot combine --tags=source=... and --source",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--tags=source=xyz,source=notxyz",
			},
			"cannot specify multiple tags named source",
		},
	}

	for _, tc := range testCases {
		tc.check(t)
	}
}

func getXstatsSenderType(t *testing.T, s Stats) string {
	xstats, ok := s.(*xStats)
	if !ok {
		t.Errorf("Stats %#v is not an *xStats", s)
		return ""
	}

	return reflect.TypeOf(xstats.sender).String()
}

func TestFromFlagsMake(t *testing.T) {
	fs := tbnflag.NewTestFlagSet()
	ff := NewFromFlags(fs)
	err := fs.Parse([]string{
		"--backends=dogstatsd,statsd",
		"--dogstatsd.host=localhost",
		"--dogstatsd.port=8000",
		"--statsd.host=localhost",
		"--statsd.port=9000",
	})
	assert.Nil(t, err)

	assert.Nil(t, ff.Validate())
	stats, err := ff.Make()
	assert.Nil(t, err)

	multiStats, ok := stats.(multiStats)
	assert.True(t, ok)
	assert.Equal(t, len(multiStats), 2)
	assert.Equal(t, getXstatsSenderType(t, multiStats[0]), "*dogstatsd.sender")
	assert.Equal(t, getXstatsSenderType(t, multiStats[1]), "*statsd.sender")
	assert.Nil(t, multiStats.Close())
}

func TestFromFlagsMakeWithLatching(t *testing.T) {
	fs := tbnflag.NewTestFlagSet()
	ff := NewFromFlags(fs)
	err := fs.Parse([]string{
		"--backends=dogstatsd,statsd",
		"--dogstatsd.host=localhost",
		"--dogstatsd.port=8000",
		"--dogstatsd.latch=true",
		"--statsd.host=localhost",
		"--statsd.port=9000",
	})
	assert.Nil(t, err)

	assert.Nil(t, ff.Validate())
	stats, err := ff.Make()
	assert.Nil(t, err)

	multiStats, ok := stats.(multiStats)
	assert.True(t, ok)
	assert.Equal(t, len(multiStats), 2)
	assert.Equal(t, getXstatsSenderType(t, multiStats[0]), "*stats.latchingSender")
	assert.Equal(t, getXstatsSenderType(t, multiStats[1]), "*statsd.sender")
	assert.Nil(t, multiStats.Close())
}

func TestFromFlagsMakeAddsTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockStats := NewMockStats(ctrl)

	mockStatsFromFlags := newMockStatsFromFlags(ctrl)
	mockStatsFromFlags.EXPECT().Make().AnyTimes().Return(mockStats, nil)

	ff := &fromFlags{
		backends: tbnflag.NewStringsWithConstraint(
			"mock",
		),
		tags: tbnflag.NewStrings(),
		statsFromFlagses: map[string]statsFromFlags{
			"mock": mockStatsFromFlags,
		},
	}
	ff.backends.Set("mock")

	mockStats.EXPECT().AddTags()
	s, err := ff.Make()
	assert.NonNil(t, s)
	assert.Nil(t, err)

	ff.tags.ResetDefault("a=b", "c=d")
	mockStats.EXPECT().AddTags(NewKVTag("a", "b"), NewKVTag("c", "d"))
	s, err = ff.Make()
	assert.NonNil(t, s)
	assert.Nil(t, err)

	ff.tags.ResetDefault("a=b", "source=s", "node=n", "c=d")
	mockStats.EXPECT().AddTags(
		NewKVTag("a", "b"),
		NewKVTag("source", "s"),
		NewKVTag("node", "n"),
		NewKVTag("c", "d"),
	)
	s, err = ff.Make()
	assert.NonNil(t, s)
	assert.Nil(t, err)

	ff.tags.ResetDefault("a=b", "c=d")
	ff.sourceTag = "s"
	ff.nodeTag = "n"
	mockStats.EXPECT().AddTags(NewKVTag("a", "b"), NewKVTag("c", "d"))
	mockStats.EXPECT().AddTags(NewKVTag("source", "s"))
	mockStats.EXPECT().AddTags(NewKVTag("node", "n"))
	s, err = ff.Make()
	assert.NonNil(t, s)
	assert.Nil(t, err)
}
