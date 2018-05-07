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
	"reflect"
	"regexp"

	"github.com/golang/mock/gomock"
	"github.com/turbinelabs/nonstdlib/executor"
)

// Matcher creates a gomock.Matcher for a Stats implementation
// generated by this package only. It does not work with arbitrary
// Stats implementations.
func Matcher(expected Stats) gomock.Matcher {
	switch s := expected.(type) {
	case *xStats:
		return &xstatsEqual{expected: s}
	case *apiStats:
		return &apiStatsEqual{expected: s}
	default:
		panic(fmt.Sprintf("unknown Stats implementation type %T", expected))
	}
}

// xstatsEqual is a Matcher for xStats implementations of Stats.
type xstatsEqual struct {
	expected *xStats
}

func (s xstatsEqual) Matches(x interface{}) bool {
	got, ok := x.(*xStats)
	if !ok {
		return false
	}

	return reflect.DeepEqual(s.expected.xstater, got.xstater) &&
		reflect.DeepEqual(s.expected.sender, got.sender) &&
		s.expected.cleaner.tagDelim == got.cleaner.tagDelim &&
		s.expected.cleaner.scopeDelim == got.cleaner.scopeDelim &&
		reflect.ValueOf(s.expected.cleaner.cleanStatName).Pointer() ==
			reflect.ValueOf(got.cleaner.cleanStatName).Pointer() &&
		reflect.ValueOf(s.expected.cleaner.cleanTagName).Pointer() ==
			reflect.ValueOf(got.cleaner.cleanTagName).Pointer()
}

func (s xstatsEqual) String() string {
	return fmt.Sprintf("xstatsEqual(%+v)", s.expected)
}

// apiStatsEqual is a Matcher for apiStats implementations of Stats.
type apiStatsEqual struct {
	expected *apiStats
}

func (s apiStatsEqual) Matches(x interface{}) bool {
	got, ok := x.(*apiStats)
	if !ok {
		fmt.Printf("wrong got type: %+v (%T)\n", x, x)
		return false
	}

	return reflect.DeepEqual(s.expected.apiSender, got.apiSender)
}

func (s apiStatsEqual) String() string {
	return fmt.Sprintf("apiStatsEqual(%+v)", s.expected)
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

	submatcher := Matcher(expected.stats)
	return submatcher.Matches(got.stats)
}

func (d DiagnosticsCallbackEqual) String() string {
	if expected, ok := d.Expected.(*statsDiagnosticsCallback); ok {
		return fmt.Sprintf(
			"DiagnosticsCallbackEqual(%s)",
			Matcher(expected.stats).String(),
		)
	}
	return "DiagnosticsCallbackEqual(invalid; expected is not a *statsDiagnosticsCallback)"
}

// TagMatches creates a Matcher that matches a Tag with the given key
// and a value regular expression.
func TagMatches(key, valueRegex string) gomock.Matcher {
	return tagMatches{
		key:        key,
		valueRegex: regexp.MustCompile(valueRegex),
	}
}

type tagMatches struct {
	key        string
	valueRegex *regexp.Regexp
}

func (m tagMatches) Matches(x interface{}) bool {
	tag, ok := x.(Tag)
	if !ok {
		return false
	}

	return m.key == tag.K && m.valueRegex.MatchString(tag.V)
}

func (m tagMatches) String() string {
	return fmt.Sprintf("tagMatches(%s=~/%s/)", m.key, m.valueRegex.String())
}
