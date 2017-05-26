package stats

import (
	"time"

	"github.com/turbinelabs/nonstdlib/executor"
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

func (sdc *statsDiagnosticsCallback) TaskStarted(numTasks int) {
	sdc.stats.Count("tasks", float64(numTasks))
}

func (sdc *statsDiagnosticsCallback) TaskCompleted(r executor.AttemptResult, d time.Duration) {
	tag := NewKVTag("result", r.String())
	sdc.stats.Count("tasks_completed", 1.0, tag)
	sdc.stats.Timing("task_duration", d, tag)
}

func (sdc *statsDiagnosticsCallback) AttemptStarted(d time.Duration) {
	sdc.stats.Count("attempts", 1.0)
	sdc.stats.Timing("attempt_delay", d)
}

func (sdc *statsDiagnosticsCallback) AttemptCompleted(r executor.AttemptResult, d time.Duration) {
	tag := NewKVTag("result", r.String())
	sdc.stats.Count("attempts_completed", 1.0, tag)
	sdc.stats.Timing("attempt_duration", d, tag)
}

func (sdc *statsDiagnosticsCallback) CallbackDuration(d time.Duration) {
	sdc.stats.Timing("callback_duration", d)
}
