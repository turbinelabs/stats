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
	"testing"
	"time"

	tbntime "github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/test/assert"
)

func TestPublishOnInterval(t *testing.T) {
	tbntime.WithCurrentTimeFrozen(func(cs tbntime.ControlledSource) {
		ch := make(chan string, 1)
		fn := func() { ch <- "tick" }

		err := publishOnInterval(1*time.Minute, fn, cs)
		assert.Nil(t, err)
		assert.ChannelEmpty(t, ch)

		cs.Advance(1 * time.Minute)
		assert.Equal(t, <-ch, "tick")
		assert.ChannelEmpty(t, ch)

		cs.Advance(1 * time.Minute)
		assert.Equal(t, <-ch, "tick")
		assert.ChannelEmpty(t, ch)
	})
}

func TestPublishOnIntervalError(t *testing.T) {
	err := PublishOnInterval(
		MinimumStatsInterval-1,
		func() {
			assert.Failed(t, "unexpected call to publishFn")
		},
	)
	assert.ErrorContains(t, err, "less than minimum stats interval")
}
