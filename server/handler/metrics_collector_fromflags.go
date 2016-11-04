package handler

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

import (
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

	return ff
}

type metricsCollectorFromFlags struct {
	forwarderFromFlags forwarder.FromFlags
}

func (ff *metricsCollectorFromFlags) Validate() error {
	return ff.forwarderFromFlags.Validate()
}

func (ff *metricsCollectorFromFlags) Make(log *log.Logger) (MetricsCollector, error) {
	fwd, err := ff.forwarderFromFlags.Make(log)
	if err != nil {
		return nil, err
	}

	return NewMetricsCollector(fwd), nil
}
