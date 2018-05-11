/*
Copyright 2018 Turbine Labs, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stats

import (
	"fmt"
	"time"

	tbntime "github.com/turbinelabs/nonstdlib/time"
)

// MinimumStatsInterval is the minimum interval at which PublishOnInterval will
// publish stats.
const MinimumStatsInterval = 1 * time.Second

// PublishOnInterval calls some function that will publish stats on a given
// interval. If the interval is less than MinimumStatsInterval an error is
// returned.
func PublishOnInterval(
	interval time.Duration,
	publishFn func(),
) error {
	return publishOnInterval(interval, publishFn, tbntime.NewSource())
}

func publishOnInterval(
	interval time.Duration,
	publishFn func(),
	source tbntime.Source,
) error {
	if interval < MinimumStatsInterval {
		return fmt.Errorf(
			"%v is less than minimum stats interval of %v",
			interval,
			MinimumStatsInterval,
		)
	}

	tmr := source.NewTimer(interval)

	go func() {
		for {
			<-tmr.C()
			publishFn()
			tmr.Reset(interval)
		}
	}()

	return nil
}
