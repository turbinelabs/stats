package stats

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/turbinelabs/nonstdlib/executor"
	"github.com/turbinelabs/test/assert"
)

func testDiagnosticsCallback(
	t *testing.T,
	setupExpectations func(*MockStats),
	invokeDiagnostics func(diag executor.DiagnosticsCallback),
) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockStats := NewMockStats(ctrl)
	diag := NewStatsDiagnosticsCallback(mockStats)

	setupExpectations(mockStats)
	invokeDiagnostics(diag)
}

func forEachAttemptResult(f func(executor.AttemptResult)) {
	for _, r := range []executor.AttemptResult{
		executor.AttemptSuccess,
		executor.AttemptTimeout,
		executor.AttemptGlobalTimeout,
		executor.AttemptCancellation,
		executor.AttemptError,
	} {
		f(r)
	}
}

func TestStatsDiagnosticsCallbackTaskStarted(t *testing.T) {
	testDiagnosticsCallback(
		t,
		func(stats *MockStats) {
			stats.EXPECT().Count("tasks", 2.0)
		},
		func(diag executor.DiagnosticsCallback) {
			diag.TaskStarted(2)
		},
	)

}

func TestStatsDiagnosticsCallbackTaskCompleted(t *testing.T) {
	testDiagnosticsCallback(
		t,
		func(stats *MockStats) {
			forEachAttemptResult(func(r executor.AttemptResult) {
				stats.EXPECT().Count(
					"tasks_completed",
					1.0,
					NewKVTag("result", r.String()),
				)
				stats.EXPECT().Timing(
					"task_duration",
					time.Duration(r)*time.Second,
					NewKVTag("result", r.String()),
				)
			})
		},
		func(diag executor.DiagnosticsCallback) {
			forEachAttemptResult(func(r executor.AttemptResult) {
				diag.TaskCompleted(r, time.Duration(r)*time.Second)
			})
		},
	)

}

func TestStatsDiagnosticsCallbackAttemptStarted(t *testing.T) {
	testDiagnosticsCallback(
		t,
		func(stats *MockStats) {
			stats.EXPECT().Count("attempts", 1.0)
			stats.EXPECT().Timing("attempt_delay", time.Minute)
		},
		func(diag executor.DiagnosticsCallback) {
			diag.AttemptStarted(time.Minute)
		},
	)
}

func TestStatsDiagnosticsCallbackAttemptCompleted(t *testing.T) {
	testDiagnosticsCallback(
		t,
		func(stats *MockStats) {
			forEachAttemptResult(func(r executor.AttemptResult) {
				stats.EXPECT().Count(
					"attempts_completed",
					1.0,
					NewKVTag("result", r.String()),
				)
				stats.EXPECT().Timing(
					"attempt_duration",
					time.Duration(r)*time.Second,
					NewKVTag("result", r.String()),
				)
			})
		},
		func(diag executor.DiagnosticsCallback) {
			forEachAttemptResult(func(r executor.AttemptResult) {
				diag.AttemptCompleted(r, time.Duration(r)*time.Second)
			})
		},
	)
}

func TestStatsDiagnosticsCallbackCallbackDuration(t *testing.T) {
	testDiagnosticsCallback(
		t,
		func(stats *MockStats) {
			stats.EXPECT().Timing("callback_duration", time.Millisecond)
		},
		func(diag executor.DiagnosticsCallback) {
			diag.CallbackDuration(time.Millisecond)
		},
	)
}
