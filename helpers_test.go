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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	tbntime "github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/test/assert"
)

type srError struct{}

func (e srError) Error() string { return "i can haz failure?" }

func resetLatencyTimeSource() {
	latencyTimeSource = tbntime.NewSource()
}

func TestLatency(t *testing.T) {
	defer resetLatencyTimeSource()

	tbntime.WithCurrentTimeFrozen(func(cs tbntime.ControlledSource) {
		ctrl := gomock.NewController(assert.Tracing(t))
		defer ctrl.Finish()

		latencyTimeSource = cs

		mockStats := NewMockStats(ctrl)

		f := Latency(mockStats)

		cs.Advance(100 * time.Millisecond)

		mockStats.EXPECT().Timing(LatencyStat, 100*time.Millisecond)

		f()
	})
}

func TestLatencyWithTags(t *testing.T) {
	defer resetLatencyTimeSource()

	tbntime.WithCurrentTimeFrozen(func(cs tbntime.ControlledSource) {
		ctrl := gomock.NewController(assert.Tracing(t))
		defer ctrl.Finish()

		latencyTimeSource = cs

		mockStats := NewMockStats(ctrl)

		f := Latency(mockStats, NewKVTag("a", "b"), NewKVTag("c", "d"))

		cs.Advance(100 * time.Millisecond)

		mockStats.EXPECT().Timing(
			LatencyStat,
			100*time.Millisecond,
			[]Tag{NewKVTag("a", "b"), NewKVTag("c", "d")},
		)

		f()
	})
}

func TestSuccessRate(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockStats := NewMockStats(ctrl)

	mockStats.EXPECT().Count(RequestStat, 1.0)
	mockStats.EXPECT().Count(SuccessStat, 1.0)
	SuccessRate(mockStats, nil)

	mockStats.EXPECT().Count(RequestStat, 1.0)
	mockStats.EXPECT().Count(FailureStat, 1.0, NewKVTag(ErrorTypeTag, "stats.srError"))
	SuccessRate(mockStats, &srError{})
}

func TestSuccessRateWithTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockStats := NewMockStats(ctrl)

	mockStats.EXPECT().Count(RequestStat, 1.0, []Tag{NewKVTag("a", "b"), NewKVTag("c", "d")})
	mockStats.EXPECT().Count(SuccessStat, 1.0, []Tag{NewKVTag("a", "b"), NewKVTag("c", "d")})
	SuccessRate(mockStats, nil, NewKVTag("a", "b"), NewKVTag("c", "d"))

	mockStats.EXPECT().Count(RequestStat, 1.0, []Tag{NewKVTag("a", "b"), NewKVTag("c", "d")})
	mockStats.EXPECT().Count(
		FailureStat,
		1.0,
		[]Tag{NewKVTag("a", "b"), NewKVTag("c", "d"), NewKVTag(ErrorTypeTag, "stats.srError")},
	)
	SuccessRate(mockStats, &srError{}, NewKVTag("a", "b"), NewKVTag("c", "d"))
}

func TestLatencyWithSuccessRate(t *testing.T) {
	defer resetLatencyTimeSource()

	tbntime.WithCurrentTimeFrozen(func(cs tbntime.ControlledSource) {
		ctrl := gomock.NewController(assert.Tracing(t))
		defer ctrl.Finish()

		latencyTimeSource = cs

		mockStats := NewMockStats(ctrl)

		f := LatencyWithSuccessRate(mockStats)
		cs.Advance(100 * time.Millisecond)

		mockStats.EXPECT().Timing(LatencyStat, 100*time.Millisecond)
		mockStats.EXPECT().Count(RequestStat, 1.0)
		mockStats.EXPECT().Count(SuccessStat, 1.0)
		f(nil)

		f = LatencyWithSuccessRate(mockStats)
		cs.Advance(100 * time.Millisecond)

		mockStats.EXPECT().Timing(LatencyStat, 100*time.Millisecond)
		mockStats.EXPECT().Count(RequestStat, 1.0)
		mockStats.EXPECT().Count(FailureStat, 1.0, NewKVTag(ErrorTypeTag, "stats.srError"))
		f(&srError{})
	})

}

func TestLatencyWithSuccessRateWithTags(t *testing.T) {
	defer resetLatencyTimeSource()

	tbntime.WithCurrentTimeFrozen(func(cs tbntime.ControlledSource) {
		ctrl := gomock.NewController(assert.Tracing(t))
		defer ctrl.Finish()

		latencyTimeSource = cs

		mockStats := NewMockStats(ctrl)

		f := LatencyWithSuccessRate(mockStats, NewKVTag("a", "b"))
		cs.Advance(100 * time.Millisecond)

		mockStats.EXPECT().Timing(LatencyStat, 100*time.Millisecond, NewKVTag("a", "b"))
		mockStats.EXPECT().Count(RequestStat, 1.0, NewKVTag("a", "b"))
		mockStats.EXPECT().Count(SuccessStat, 1.0, NewKVTag("a", "b"))
		f(nil)

		f = LatencyWithSuccessRate(mockStats, NewKVTag("a", "b"))
		cs.Advance(100 * time.Millisecond)

		mockStats.EXPECT().Timing(LatencyStat, 100*time.Millisecond, NewKVTag("a", "b"))
		mockStats.EXPECT().Count(RequestStat, 1.0, NewKVTag("a", "b"))
		mockStats.EXPECT().Count(
			FailureStat,
			1.0,
			[]Tag{NewKVTag("a", "b"), NewKVTag(ErrorTypeTag, "stats.srError")},
		)
		f(&srError{})
	})
}

func TestSanitizeErrorType(t *testing.T) {
	testCases := []struct {
		err      error
		expected string
	}{
		{
			err:      errors.New("boom"),
			expected: "errors.errorString",
		},
		{
			err:      &srError{},
			expected: "stats.srError",
		},
		{
			err:      srError{},
			expected: "stats.srError",
		},
	}

	for _, tc := range testCases {
		assert.Equal(t, SanitizeErrorType(tc.err), tc.expected)
	}
}
