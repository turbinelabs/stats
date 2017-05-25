package stats

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/rs/xstats/statsd"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
)

const (
	defaultHost = "127.0.0.1"
	defaultPort = 8125
)

var statsdCleaner = cleaner{
	cleanStatName: stripColons,
	cleanTagName:  strip,
	scopeDelim:    ".",
}

type statsdFromFlags struct {
	scope         string
	host          string
	port          int
	flushInterval time.Duration
}

func newStatsdFromFlags(fs tbnflag.FlagSet, scope string) *statsdFromFlags {
	ff := &statsdFromFlags{scope: scope}
	scoped := fs.Scope(scope, "")

	scoped.StringVar(
		&ff.host,
		"host",
		defaultHost,
		"Specifies the destination host for stats.",
	)

	scoped.IntVar(
		&ff.port,
		"port",
		defaultPort,
		"Specifies the destination port for stats.",
	)

	scoped.DurationVar(
		&ff.flushInterval,
		"flush-interval",
		defaultFlushInterval,
		"Specifies the interval between stats flushes.",
	)

	return ff
}

func (ff *statsdFromFlags) Make() (Stats, error) {
	w, err := ff.mkUDPWriter()
	if err != nil {
		return nil, err
	}
	return newFromSender(statsd.New(w, ff.flushInterval), statsdCleaner), nil
}

func (ff *statsdFromFlags) Validate() error {
	addr := fmt.Sprintf("%s:%d", ff.host, ff.port)

	if _, _, err := tbnstrings.SplitHostPort(addr); err != nil {
		return fmt.Errorf(
			"--%s.host or --%s.port is invalid: %s",
			ff.scope,
			ff.scope,
			err.Error(),
		)
	}

	if ff.flushInterval <= 0*time.Second {
		return fmt.Errorf("--%s.flush-interval must be greater than zero", ff.scope)
	}
	return nil
}

func (ff *statsdFromFlags) mkUDPWriter() (io.Writer, error) {
	addr := fmt.Sprintf("%s:%d", ff.host, ff.port)
	w, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return w, nil
}
