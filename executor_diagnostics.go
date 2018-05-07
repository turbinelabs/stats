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

	"github.com/turbinelabs/nonstdlib/executor"
)

const (
	tasksStat             = "tasks"
	tasksCompletedStat    = "tasks_completed"
	taskDurationStat      = "task_duration"
	attemptsStat          = "attempts"
	attemptDelayStat      = "attempt_delay"
	attemptsCompletedStat = "attempts_completed"
	attemptDurationStat   = "attempt_duration"
	callbackDurationStat  = "callback_duration"

	resultTag = "result"
)

type statsDiagnosticsCallback struct {
	stats Stats
}

// NewStatsDiagnosticsCallback wraps the given Stats within an
// implementation of executor.DiagnosticsCallback to records tasks
// started, tasks completed, task duration, attempts started, attempt
// delay, attempts completed, attempt duration, and callback duration.
func NewStatsDiagnosticsCallback(s Stats) executor.DiagnosticsCallback {
	return &statsDiagnosticsCallback{s}
}

// DiagnosticsCallbackStats returns a list of all possible stats
// generated.
func DiagnosticsCallbackStats() []string {
	return []string{
		tasksStat,
		tasksCompletedStat,
		taskDurationStat,
		attemptsStat,
		attemptDelayStat,
		attemptsCompletedStat,
		attemptDurationStat,
		callbackDurationStat,
	}
}

// DiagnosticsCallbackTags returns a list of all possible tags generated.
func DiagnosticsCallbackTags() []string {
	return []string{resultTag}
}

func (sdc *statsDiagnosticsCallback) TaskStarted(numTasks int) {
	sdc.stats.Count(tasksStat, float64(numTasks))
}

func (sdc *statsDiagnosticsCallback) TaskCompleted(r executor.AttemptResult, d time.Duration) {
	tag := NewKVTag(resultTag, r.String())
	sdc.stats.Count(tasksCompletedStat, 1.0, tag)
	sdc.stats.Timing(taskDurationStat, d, tag)
}

func (sdc *statsDiagnosticsCallback) AttemptStarted(d time.Duration) {
	sdc.stats.Count(attemptsStat, 1.0)
	sdc.stats.Timing(attemptDelayStat, d)
}

func (sdc *statsDiagnosticsCallback) AttemptCompleted(r executor.AttemptResult, d time.Duration) {
	tag := NewKVTag(resultTag, r.String())
	sdc.stats.Count(attemptsCompletedStat, 1.0, tag)
	sdc.stats.Timing(attemptDurationStat, d, tag)
}

func (sdc *statsDiagnosticsCallback) CallbackDuration(d time.Duration) {
	sdc.stats.Timing(callbackDurationStat, d)
}
