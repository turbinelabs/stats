package stats

import (
	"fmt"
	"time"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
)

type latchingSenderFromFlags struct {
	flagScope   string
	enabled     bool
	latchWindow time.Duration
	minBucket   float64
	numBuckets  int
}

func newLatchingSenderFromFlags(fs tbnflag.FlagSet) *latchingSenderFromFlags {
	scoped := fs.Scope("latch", "")

	ff := &latchingSenderFromFlags{flagScope: scoped.GetScope()}

	fs.BoolVar(
		&ff.enabled,
		"latch",
		false,
		"Specifies whether stats are accumulated over a window before being sent to the backend.",
	)

	scoped.DurationVar(
		&ff.latchWindow,
		"window",
		DefaultLatchWindow,
		"Specifies the period of time over which stats are latched. Must be greater than 0.",
	)
	scoped.Float64Var(
		&ff.minBucket,
		"base-value",
		DefaultHistogramBaseValue,
		"Specifies the upper bound of the first bucket used for accumulating histograms. Each subsequent bucket's upper bound is double the previous bucket's. For timings this value is taken to be in units of seconds. Must be greater than 0.",
	)
	scoped.IntVar(
		&ff.numBuckets,
		"buckets",
		DefaultHistogramNumBuckets,
		"Specifies the number of buckets used for accumulating histograms. Must be greater than 1.",
	)

	return ff
}

func (ff *latchingSenderFromFlags) Validate() error {
	if ff.latchWindow <= 0 {
		return fmt.Errorf("--%swindow must be greater than 0", ff.flagScope)
	}

	if ff.minBucket <= 0.0 {
		return fmt.Errorf("--%sbase-value must be greater than 0", ff.flagScope)
	}

	if ff.numBuckets <= 1 {
		return fmt.Errorf("--%sbuckets must be greater than 1", ff.flagScope)
	}

	return nil
}

func (ff *latchingSenderFromFlags) Make(underlying xstatsSender, c cleaner) xstatsSender {
	if ff.enabled {
		return newLatchingSender(
			underlying,
			c,
			latchWindow(ff.latchWindow),
			latchBuckets(ff.minBucket, ff.numBuckets),
		)
	}

	return underlying
}
