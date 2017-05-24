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

var statsdCleaner = cleaner{
	cleanTagName:  identity,
	cleanStatName: identity,
	scopeDelim:    ".",
	tagDelim:      "=",
}

type statsdFromFlags struct {
	scope         string
	addr          string
	flushInterval time.Duration
}

func newStatsdFromFlags(fs tbnflag.FlagSet, scope string) *statsdFromFlags {
	ff := &statsdFromFlags{scope: scope}
	scoped := fs.Scope(scope, "")

	scoped.StringVar(
		&ff.addr,
		"addr",
		"127.0.0.1:8125",
		"Specifies the destination address for stats.",
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
	if _, _, err := tbnstrings.SplitHostPort(ff.addr); err != nil {
		return fmt.Errorf("--%s.addr is invalid: %s", ff.scope, err.Error())
	}

	if ff.flushInterval <= 0*time.Second {
		return fmt.Errorf("--%s.flush-interval must be greater than zero", ff.scope)
	}
	return nil
}

func (ff *statsdFromFlags) mkUDPWriter() (io.Writer, error) {
	w, err := net.Dial("udp", ff.addr)
	if err != nil {
		return nil, err
	}
	return w, nil
}
