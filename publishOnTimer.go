package stats

import (
	"fmt"
	"time"
)

const MinimumStatsInterval = 1 * time.Second

// PublishOnInterval calls some function that will publish stats on a given
// interval. If the interval is less than MinimumStatsInterval an error is
// returned.
func PublishOnInterval(
	interval time.Duration,
	publishFn func(),
) error {
	if interval < MinimumStatsInterval {
		return fmt.Errorf(
			"%v is less than minimum stats interval of %v\n",
			interval,
			MinimumStatsInterval,
		)
	}

	tmr := time.NewTimer(interval)

	trigger := func() {
		for {
			<-tmr.C
			publishFn()
			tmr.Reset(interval)
		}
	}

	go trigger()
	return nil
}
