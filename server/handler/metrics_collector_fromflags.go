package handler

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

import (
	"errors"
	"flag"
	"log"

	"github.com/turbinelabs/logparser/forwarder"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
)

// MetricsCollectorFromFlags constructs a MetricsCollector from
// command line flags.
type MetricsCollectorFromFlags interface {
	// Validates the metrics collector command line flags.
	Validate() error

	// Constructs a new MetricsCollector with the given
	// log.Logger.
	Make(*log.Logger) (MetricsCollector, error)
}

// NewMetricsCollectorFromFlags constructs a new
// MetricsCollectorFromFlags.
func NewMetricsCollectorFromFlags(flagset *flag.FlagSet) MetricsCollectorFromFlags {
	ff := &metricsCollectorFromFlags{}

	pfs := tbnflag.NewPrefixedFlagSet(
		flagset,
		"stats",
		"stats forwarder",
	)

	ff.forwarderFromFlags = forwarder.NewFromFlags(
		pfs,
		forwarder.SetDefaultForwarderType(forwarder.WavefrontForwarderType),
		forwarder.DisableTurbineAPIForwarding(),
	)

	pfs.IntVar(
		&ff.bufferSize,
		"buffer-size",
		0,
		"Sets the size of the buffer used when forwarding stats. If 0, stats forwarding requests will not complete until metrics have been forwarded. If non-zero and the buffer is full, forwarding requests will fail.",
	)

	return ff
}

type metricsCollectorFromFlags struct {
	forwarderFromFlags forwarder.FromFlags
	bufferSize         int
}

func (ff *metricsCollectorFromFlags) Validate() error {
	if ff.bufferSize < 0 {
		return errors.New("buffer-size must not be negative")
	}

	return ff.forwarderFromFlags.Validate()
}

func (ff *metricsCollectorFromFlags) Make(log *log.Logger) (MetricsCollector, error) {
	fwd, err := ff.forwarderFromFlags.Make(log)
	if err != nil {
		return nil, err
	}

	if ff.bufferSize > 0 {
		fwd = forwarder.NewAsyncForwarder(log, fwd, ff.bufferSize)
	}

	return NewMetricsCollector(fwd), nil
}
