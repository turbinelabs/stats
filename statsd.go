package stats

import (
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/rs/xstats/statsd"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
)

const (
	defaultHost          = "127.0.0.1"
	defaultPort          = 8125
	defaultFlushInterval = 5 * time.Second
	defaultMaxPacketLen  = 8192 // assume jumbo ethernet frames that handle 8k payload
)

var statsdCleaner = cleaner{
	cleanStatName: stripColons,
	cleanTagName:  strip,
	cleanTagValue: strip,
	scopeDelim:    ".",
}

var stdoutWriter io.Writer = os.Stdout

type statsdFromFlags struct {
	flagScope     string
	host          string
	port          int
	maxPacketLen  int
	flushInterval time.Duration
	scope         string
	lsff          *latchingSenderFromFlags
	debug         bool
}

func newStatsdFromFlags(fs tbnflag.FlagSet) *statsdFromFlags {
	ff := &statsdFromFlags{
		flagScope: fs.GetScope(),
		lsff:      newLatchingSenderFromFlags(fs, false),
	}

	fs.StringVar(
		&ff.host,
		"host",
		defaultHost,
		"Specifies the destination host for stats.",
	)

	fs.IntVar(
		&ff.port,
		"port",
		defaultPort,
		"Specifies the destination port for stats.",
	)

	fs.IntVar(
		&ff.maxPacketLen,
		"max-packet-len",
		defaultMaxPacketLen,
		"Specifies the maximum number of payload `bytes` sent per flush. If necessary, flushes will occur before the flush interval to prevent payloads from exceeding this size. The size does not include IP and UDP header bytes. Stats may not be delivered if the total size of the headers and payload exceeds the network's MTU.",
	)

	fs.DurationVar(
		&ff.flushInterval,
		"flush-interval",
		defaultFlushInterval,
		"Specifies the `duration` between stats flushes.",
	)

	fs.StringVar(
		&ff.scope,
		"scope",
		"",
		"If specified, prepends the given scope to metric names.",
	)

	fs.BoolVar(
		&ff.debug,
		"debug",
		false,
		"If enabled, logs the stats data on stdout.",
	)

	return ff
}

func (ff *statsdFromFlags) Validate() error {
	addr := fmt.Sprintf("%s:%d", ff.host, ff.port)

	if _, _, err := tbnstrings.SplitHostPort(addr); err != nil {
		return fmt.Errorf(
			"--%shost or --%sport is invalid: %s",
			ff.flagScope,
			ff.flagScope,
			err.Error(),
		)
	}

	if ff.flushInterval <= 0*time.Second {
		return fmt.Errorf("--%sflush-interval must be greater than zero", ff.flagScope)
	}

	return ff.lsff.Validate()
}

func (ff *statsdFromFlags) Make() (Stats, error) {
	w, err := ff.mkUDPWriter()
	if err != nil {
		return nil, err
	}

	underlying := ff.lsff.Make(
		statsd.NewMaxPacket(w, ff.flushInterval, ff.maxPacketLen),
		statsdCleaner,
	)

	return newFromSender(underlying, statsdCleaner, ff.scope, true), nil
}

func (ff *statsdFromFlags) mkUDPWriter() (io.Writer, error) {
	var (
		w   io.Writer
		err error
	)

	addr := fmt.Sprintf("%s:%d", ff.host, ff.port)
	w, err = net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}

	if ff.debug {
		w = &debugWriter{w, stdoutWriter}
	}

	return w, nil
}

// debugWriter differs from io.MultiWriter in that it ignores short
// writes and errors on its debug Writer.
type debugWriter struct {
	underlying, debug io.Writer
}

func (dw *debugWriter) Write(b []byte) (int, error) {
	defer dw.debug.Write(b)
	return dw.underlying.Write(b)
}
