package stats

import (
	"errors"
	"flag"
	"fmt"
	reflect "reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	apiflags "github.com/turbinelabs/api/client/flags"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/test/assert"
)

const uuidRegex = "[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}"

type validateTestCase struct {
	args                []string
	expectErrorContains string
}

func (vtc *validateTestCase) check(t *testing.T) {
	desc := strings.Join(vtc.args, " ")
	assert.Group(desc, t, func(g *assert.G) {
		fs := flag.NewFlagSet("stats test flags", flag.ContinueOnError)
		ff := NewFromFlags(tbnflag.Wrap(fs), EnableAPIStatsBackend())
		err := fs.Parse(vtc.args)
		if strings.HasPrefix(vtc.expectErrorContains, "PARSER:") {
			expectedErr := strings.TrimSpace(vtc.expectErrorContains[7:])
			assert.ErrorContains(t, err, expectedErr)
			return
		}

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

	mockZoneFromFlags := apiflags.NewMockZoneFromFlags(ctrl)
	mockZoneFromFlags.EXPECT().Name().Return("zone")

	fs := tbnflag.NewTestFlagSet()

	ff := NewFromFlags(
		fs,
		EnableAPIStatsBackend(),
		APIStatsOptions(
			SetStatsClientFromFlags(mockStatsClientFromFlags),
			SetZoneFromFlags(mockZoneFromFlags),
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
			"PARSER: -prometheus.addr: address nope: missing port",
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

		// node, source, unique-source, and tags
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
			"cannot combine --tags=node=... with --node",
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
				"--node=" + strings.Repeat("X", maxSourceLen+1),
			},
			"--node or --tags=node=... may not be longer than 256 bytes",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--tags=node=" + strings.Repeat("X", maxSourceLen+1),
			},
			"--node or --tags=node=... may not be longer than 256 bytes",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--source=xyz",
				"--tags=source=notxyz",
			},
			"cannot combine --tags=source=... with --source or --unique-source",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--tags=source=xyz,source=notxyz",
			},
			"cannot specify multiple tags named source",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--unique-source=xyz",
				"--tags=source=notxyz",
			},
			"cannot combine --tags=source=... with --source or --unique-source",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--source=xyz",
				"--unique-source=pdq",
			},
			"cannot combine --tags=source=... with --source or --unique-source",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--source=" + strings.Repeat("X", maxSourceLen+1),
			},
			"--source or --tags=source=... may not be longer than 256 bytes",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--tags=source=" + strings.Repeat("X", maxSourceLen+1),
			},
			"--source or --tags=source=... may not be longer than 256 bytes",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--unique-source=" + strings.Repeat("X", maxSourceLen+1),
			},
			"--unique-source may not be longer than 256 bytes",
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

	assert.Equal(t, ff.Node(), "")
	assert.NotEqual(t, ff.Source(), "")
}

type tagsTestCase struct {
	args                 []string
	expectedNode         string
	expectedSource       string
	expectedSourcePrefix string
}

func (ttc *tagsTestCase) check(t *testing.T) {
	desc := strings.Join(ttc.args, " ")
	assert.Group(desc, t, func(g *assert.G) {
		fs := flag.NewFlagSet("stats test flags", flag.ContinueOnError)
		ff := NewFromFlags(tbnflag.Wrap(fs))
		assert.Nil(
			g,
			fs.Parse(
				append(
					ttc.args,
					"--backends=statsd",
					"--statsd.host=localhost",
					"--statsd.port=9000",
				),
			),
		)
		assert.Nil(g, ff.Validate())
		stats, err := ff.Make()
		assert.Nil(g, err)
		defer stats.Close()

		assert.Equal(g, ff.Node(), ttc.expectedNode)
		if ttc.expectedSource != "" {
			assert.Equal(t, ff.Source(), ttc.expectedSource)
		} else {
			assert.MatchesRegex(
				g,
				ff.Source(),
				fmt.Sprintf(
					`^%s[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$`,
					ttc.expectedSourcePrefix,
				),
			)
		}
	})
}

func TestFromFlagsNodeAndSource(t *testing.T) {
	testCases := []tagsTestCase{
		// no tags
		{},

		// node set by flag
		{
			args:         []string{"--node=the-node"},
			expectedNode: "the-node",
		},

		// node set by tag
		{
			args:         []string{"--tags=node=the-node"},
			expectedNode: "the-node",
		},

		// source set by flag
		{
			args:                 []string{"--source=the-source"},
			expectedSourcePrefix: "the-source-",
		},

		// source set by tag
		{
			args:                 []string{"--tags=source=the-source"},
			expectedSourcePrefix: "the-source-",
		},

		// unique source set by flag
		{
			args:           []string{"--unique-source=the-unique-source"},
			expectedSource: "the-unique-source",
		},
	}

	for _, tc := range testCases {
		tc.check(t)
	}
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

type makeAddTagsTestCase struct {
	tags            []string
	node            string
	source          string
	uniqueSource    string
	expectedAddTags [][]interface{}
}

func (mattc *makeAddTagsTestCase) check(t *testing.T) {
	desc := strings.Join(mattc.tags, " ")
	assert.Group(desc, t, func(g *assert.G) {
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

		if mattc.node != "" {
			ff.nodeTag = mattc.node
		}

		if mattc.source != "" {
			ff.sourceTag = mattc.source
		}

		if mattc.uniqueSource != "" {
			ff.uniqueSourceTag = mattc.uniqueSource
		}

		if len(mattc.tags) > 0 {
			ff.tags.ResetDefault(mattc.tags...)
		}

		for _, tags := range mattc.expectedAddTags {
			if tags == nil {
				mockStats.EXPECT().AddTags().Times(2)
			} else {
				mockStats.EXPECT().AddTags(tags...).Times(2)
			}
		}

		s, err := ff.Make()
		assert.NonNil(t, s)
		assert.Nil(t, err)

		currentSource, currentNode := ff.Source(), ff.Node()

		// Make another Stats with the same config
		s, err = ff.Make()
		assert.NonNil(t, s)
		assert.Nil(t, err)

		// These should not change.
		assert.Equal(t, ff.Source(), currentSource)
		assert.Equal(t, ff.Node(), currentNode)
	})
}
func TestFromFlagsMakeAddsTags(t *testing.T) {
	testCases := []makeAddTagsTestCase{
		// no tags
		{
			expectedAddTags: [][]interface{}{
				nil,
				{TagMatches("source", uuidRegex)},
			},
		},

		// non-special tags
		{
			tags: []string{"a=b", "c=d"},
			expectedAddTags: [][]interface{}{
				{NewKVTag("a", "b"), NewKVTag("c", "d")},
				{TagMatches("source", uuidRegex)},
			},
		},

		// source, node, and tags
		{
			tags: []string{"a=b", "source=s", "node=n", "c=d"},
			expectedAddTags: [][]interface{}{
				{NewKVTag("a", "b"), NewKVTag("c", "d")},
				{NewKVTag("node", "n")},
				{TagMatches("source", "s-"+uuidRegex)},
			},
		},

		// source, node from flags
		{
			tags:   []string{"a=b", "c=d"},
			node:   "n",
			source: "s",
			expectedAddTags: [][]interface{}{
				{NewKVTag("a", "b"), NewKVTag("c", "d")},
				{NewKVTag("node", "n")},
				{TagMatches("source", "s-"+uuidRegex)},
			},
		},

		// unique source, node from flags
		{
			tags:         []string{"a=b", "c=d"},
			node:         "n",
			uniqueSource: "s",
			expectedAddTags: [][]interface{}{
				{NewKVTag("a", "b"), NewKVTag("c", "d")},
				{NewKVTag("node", "n")},
				{NewKVTag("source", "s")},
			},
		},
	}

	for _, tc := range testCases {
		tc.check(t)
	}
}
