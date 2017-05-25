package stats

import (
	"flag"
	"strings"
	"testing"

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
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		tfs := tbnflag.Wrap(fs)
		ff := NewFromFlags(tfs)
		err := fs.Parse(vtc.args)
		assert.Nil(t, err)
		if vtc.expectErrorContains != "" {
			assert.ErrorContains(t, ff.Validate(), vtc.expectErrorContains)
		} else {
			assert.Nil(t, ff.Validate())
		}
	})
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
				"--dogstatsd.flush-interval=1s",
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
	}

	for _, tc := range testCases {
		tc.check(t)
	}
}

func TestFromFlagsMake(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	tfs := tbnflag.Wrap(fs)
	ff := NewFromFlags(tfs)
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
	assert.Nil(t, multiStats.Close())
}
