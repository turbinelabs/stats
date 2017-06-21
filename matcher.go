package stats

import (
	"fmt"
	"reflect"

	"github.com/turbinelabs/nonstdlib/executor"
)

// StatsEqual is a Matcher for Stats implementations generated by this
// package only.
type StatsEqual struct {
	Expected Stats
}

func (s StatsEqual) Matches(x interface{}) bool {
	expected, ok := s.Expected.(*xStats)
	if !ok {
		return false
	}

	got, ok := x.(*xStats)
	if !ok {
		return false
	}

	return reflect.DeepEqual(expected.xstater, got.xstater) &&
		reflect.DeepEqual(expected.sender, got.sender) &&
		expected.cleaner.tagDelim == got.cleaner.tagDelim &&
		expected.cleaner.scopeDelim == got.cleaner.scopeDelim &&
		reflect.ValueOf(expected.cleaner.cleanStatName).Pointer() ==
			reflect.ValueOf(got.cleaner.cleanStatName).Pointer() &&
		reflect.ValueOf(expected.cleaner.cleanTagName).Pointer() ==
			reflect.ValueOf(got.cleaner.cleanTagName).Pointer()
}

func (s StatsEqual) String() string {
	if expected, ok := s.Expected.(*xStats); ok {
		return fmt.Sprintf("StatsEqual(%+v)", expected)
	}

	return "StatsEqual(invalid; expected is not an *xStats)"
}

// DiagnosticsCallbackEqual is a Matcher for
// executor.DiagnosticsCallback implementations generated by this
// package only.
type DiagnosticsCallbackEqual struct {
	Expected executor.DiagnosticsCallback
}

func (d DiagnosticsCallbackEqual) Matches(x interface{}) bool {
	expected, ok := d.Expected.(*statsDiagnosticsCallback)
	if !ok {
		return false
	}

	got, ok := x.(*statsDiagnosticsCallback)
	if !ok {
		return false
	}

	submatcher := StatsEqual{expected.stats}
	return submatcher.Matches(got.stats)
}

func (d DiagnosticsCallbackEqual) String() string {
	if expected, ok := d.Expected.(*statsDiagnosticsCallback); ok {
		return fmt.Sprintf(
			"DiagnosticsCallbackEqual(%s)",
			StatsEqual{expected.stats}.String(),
		)
	}
	return "DiagnosticsCallbackEqual(invalid; expected is not a *statsDiagnosticsCallback)"
}