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

func TestLatencyWithSimulatedClockReset(t *testing.T) {
	defer resetLatencyTimeSource()

	tbntime.WithCurrentTimeFrozen(func(cs tbntime.ControlledSource) {
		ctrl := gomock.NewController(assert.Tracing(t))
		defer ctrl.Finish()

		latencyTimeSource = cs

		mockStats := NewMockStats(ctrl)

		f := Latency(mockStats)

		cs.Advance(-100 * time.Millisecond)

		mockStats.EXPECT().Timing(LatencyStat, 0*time.Millisecond)

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
