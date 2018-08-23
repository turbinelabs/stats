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
	"time"
)

// eventSender is a mixin that provides noop implementations of Gauge,
// Histogram, Timing and Count, as structured event backends typically
// use Event in the Stats interface
type eventSender struct {
	scopes []string
}

func (es *eventSender) Gauge(stat string, value float64, tags ...Tag) {}

func (es *eventSender) Histogram(stat string, value float64, tags ...Tag) {}

func (es *eventSender) Timing(stat string, value time.Duration, tags ...Tag) {}

func (es *eventSender) Count(stat string, count float64, tags ...Tag) {}

func (es *eventSender) scope(scope string, scopes ...string) eventSender {
	newScopes := make([]string, 0, len(es.scopes)+1+len(scopes))
	newScopes = append(newScopes, es.scopes...)
	newScopes = append(newScopes, scope)
	newScopes = append(newScopes, scopes...)
	return eventSender{scopes: newScopes}
}
