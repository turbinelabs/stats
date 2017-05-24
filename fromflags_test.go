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
				"--dogstatsd.addr=nope",
			},
			"--dogstatsd.addr is invalid",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--dogstatsd.addr=0:8000",
				"--dogstatsd.flush-interval=0",
			},
			"--dogstatsd.flush-interval must be greater than zero",
		},
		{
			[]string{
				"--backends=dogstatsd",
				"--dogstatsd.addr=0:8000",
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
				"--statsd.addr=nope",
			},
			"--statsd.addr is invalid",
		},
		{
			[]string{
				"--backends=statsd",
				"--statsd.addr=0:8000",
				"--statsd.flush-interval=0",
			},
			"--statsd.flush-interval must be greater than zero",
		},
		{
			[]string{
				"--backends=statsd",
				"--statsd.addr=0:8000",
				"--statsd.flush-interval=1s",
			},
			"",
		},
		// wavefront
		{
			[]string{
				"--backends=wavefront",
				"--wavefront.addr=nope",
			},
			"--wavefront.addr is invalid",
		},
		{
			[]string{
				"--backends=wavefront",
				"--wavefront.addr=0:8000",
				"--wavefront.flush-interval=0",
			},
			"--wavefront.flush-interval must be greater than zero",
		},
		{
			[]string{
				"--backends=wavefront",
				"--wavefront.addr=0:8000",
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
		"--dogstatsd.addr=localhost:8000",
		"--statsd.addr=localhost:9000",
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
