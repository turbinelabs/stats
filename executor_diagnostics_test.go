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

	"github.com/golang/mock/gomock"
	"github.com/turbinelabs/nonstdlib/executor"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
	"github.com/turbinelabs/test/assert"
	"github.com/turbinelabs/test/matcher"
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
			executor.ForEachAttemptResult(func(r executor.AttemptResult) {
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
			executor.ForEachAttemptResult(func(r executor.AttemptResult) {
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
			executor.ForEachAttemptResult(func(r executor.AttemptResult) {
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
			executor.ForEachAttemptResult(func(r executor.AttemptResult) {
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

func TestDiagnosticCallbackStatsAndTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockStats := NewMockStats(ctrl)

	statNameCaptor := matcher.CaptureAll()
	tagCaptor := matcher.CaptureAll()

	mockStats.EXPECT().Count(statNameCaptor, 1.0).AnyTimes()
	mockStats.EXPECT().Count(statNameCaptor, 1.0, tagCaptor).AnyTimes()
	mockStats.EXPECT().Timing(statNameCaptor, time.Millisecond).AnyTimes()
	mockStats.EXPECT().Timing(statNameCaptor, time.Millisecond, tagCaptor).AnyTimes()

	diag := NewStatsDiagnosticsCallback(mockStats)
	executor.ForEachAttemptResult(func(r executor.AttemptResult) {
		diag.TaskStarted(1)
		diag.AttemptStarted(time.Millisecond)
		diag.AttemptCompleted(r, time.Millisecond)
		diag.TaskCompleted(r, time.Millisecond)
		diag.CallbackDuration(time.Millisecond)
	})

	capturedStatNames := tbnstrings.NewSet()
	for _, v := range statNameCaptor.V {
		statName, ok := v.(string)
		assert.True(t, ok)
		capturedStatNames.Put(statName)
	}

	capturedTagNames := tbnstrings.NewSet()
	for _, v := range tagCaptor.V {
		tag, ok := v.(Tag)
		assert.True(t, ok)
		capturedTagNames.Put(tag.K)
	}

	assert.HasSameElements(t, DiagnosticsCallbackStats(), capturedStatNames.Slice())
	assert.HasSameElements(t, DiagnosticsCallbackTags(), capturedTagNames.Slice())
}
