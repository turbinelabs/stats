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
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/golang/mock/gomock"
	"github.com/turbinelabs/test/assert"
)

func TestReadRune(t *testing.T) {
	pos := 100
	reader := strings.NewReader("a")

	r, eof, err := readRune(reader, &pos)
	assert.Equal(t, r, 'a')
	assert.False(t, eof)
	assert.Equal(t, pos, 101)
	assert.Nil(t, err)

	r, eof, err = readRune(reader, &pos)
	assert.Equal(t, r, utf8.RuneError)
	assert.True(t, eof)
	assert.Equal(t, pos, 101)
	assert.Nil(t, err)

	pos = 100
	reader = strings.NewReader(string([]byte{0x80, 0x80, 0x80, 0x80, 0x80}))

	r, eof, err = readRune(reader, &pos)
	assert.Equal(t, r, utf8.RuneError)
	assert.False(t, eof)
	assert.Equal(t, pos, 100)
	assert.ErrorContains(t, err, "char 100: invalid UTF-8 character in transformation")
}

func TestReadRuneNoEOF(t *testing.T) {
	pos := 100
	reader := strings.NewReader("a")

	r, err := readRuneNoEOF(reader, &pos)
	assert.Equal(t, r, 'a')
	assert.Equal(t, pos, 101)
	assert.Nil(t, err)

	r, err = readRuneNoEOF(reader, &pos)
	assert.Equal(t, r, utf8.RuneError)
	assert.Equal(t, pos, 101)
	assert.ErrorContains(t, err, "char 101: unexpected end of transformation")

	pos = 100
	reader = strings.NewReader(string([]byte{0x80, 0x80, 0x80, 0x80, 0x80}))

	r, err = readRuneNoEOF(reader, &pos)
	assert.Equal(t, r, utf8.RuneError)
	assert.Equal(t, pos, 100)
	assert.ErrorContains(t, err, "char 100: invalid UTF-8 character in transformation")
}

func TestReadUntilDelim(t *testing.T) {
	testCases := []struct {
		input          string
		delims         []rune
		expectedOutput string
		expectedDelim  rune
		expectedEOF    bool
		expectedErr    string
		expectedOffset int
	}{
		{
			input:          "This sentence's delimiter: a colon.",
			delims:         []rune{':'},
			expectedOutput: "This sentence's delimiter",
			expectedDelim:  ':',
			expectedOffset: 26,
		},
		{
			input:         "",
			delims:        []rune{':'},
			expectedDelim: utf8.RuneError,
			expectedEOF:   true,
			expectedErr:   "unexpected end",
		},
		{
			input:          ":",
			delims:         []rune{':'},
			expectedDelim:  ':',
			expectedErr:    "expected at least 1 character",
			expectedOffset: 1,
		},
		{
			input:          "abc",
			delims:         []rune{':'},
			expectedDelim:  utf8.RuneError,
			expectedOutput: "abc",
			expectedEOF:    true,
			expectedOffset: 3,
		},
		{
			input:          "invalid utf8 " + string([]byte{0x80, 0x80, 0x80, 0x80, 0x80}),
			delims:         []rune{':'},
			expectedDelim:  utf8.RuneError,
			expectedErr:    "invalid UTF-8",
			expectedOffset: 13,
		},
		{
			input:          `escaped \:, not escaped :`,
			delims:         []rune{':'},
			expectedOutput: `escaped :, not escaped `,
			expectedDelim:  ':',
			expectedOffset: 25,
		},
	}

	for i, testCase := range testCases {
		assert.Group(
			fmt.Sprintf("testCase[%d]: %s", i, testCase.input),
			t,
			func(g *assert.G) {
				pos := 100
				reader := strings.NewReader(testCase.input)
				s, delim, eof, err := readUntilDelim(reader, &pos, testCase.delims...)

				assert.Equal(g, s, testCase.expectedOutput)
				assert.Equal(g, delim, testCase.expectedDelim)
				assert.Equal(g, eof, testCase.expectedEOF)
				assert.Equal(g, pos, testCase.expectedOffset+100)
				if testCase.expectedErr != "" {
					assert.ErrorContains(g, err, testCase.expectedErr)
				} else {
					assert.Nil(g, err)
				}
			},
		)
	}
}

func TestReadUntilDelimNoEOF(t *testing.T) {
	testCases := []struct {
		input          string
		delims         []rune
		expectedOutput string
		expectedDelim  rune
		expectedErr    string
		expectedOffset int
	}{
		{
			input:          "This sentence's delimiter: a colon.",
			delims:         []rune{':'},
			expectedOutput: "This sentence's delimiter",
			expectedDelim:  ':',
			expectedOffset: 26,
		},
		{
			input:         "",
			delims:        []rune{':'},
			expectedDelim: utf8.RuneError,
			expectedErr:   "unexpected end",
		},
		{
			input:          ":",
			delims:         []rune{':'},
			expectedDelim:  ':',
			expectedErr:    "expected at least 1 character",
			expectedOffset: 1,
		},
		{
			input:          "abc",
			delims:         []rune{':'},
			expectedDelim:  utf8.RuneError,
			expectedOutput: "abc",
			expectedErr:    "unexpected end",
			expectedOffset: 3,
		},
		{
			input:          "invalid utf8 " + string([]byte{0x80, 0x80, 0x80, 0x80, 0x80}),
			delims:         []rune{':'},
			expectedDelim:  utf8.RuneError,
			expectedErr:    "invalid UTF-8",
			expectedOffset: 13,
		},
		{
			input:          `escaped \:, not escaped :`,
			delims:         []rune{':'},
			expectedOutput: `escaped :, not escaped `,
			expectedDelim:  ':',
			expectedOffset: 25,
		},
	}

	for i, testCase := range testCases {
		assert.Group(
			fmt.Sprintf("testCase[%d]: %s", i, testCase.input),
			t,
			func(g *assert.G) {
				pos := 100
				reader := strings.NewReader(testCase.input)
				s, delim, err := readUntilDelimNoEOF(reader, &pos, testCase.delims...)

				assert.Equal(g, s, testCase.expectedOutput)
				assert.Equal(g, delim, testCase.expectedDelim)
				assert.Equal(g, pos, testCase.expectedOffset+100)
				if testCase.expectedErr != "" {
					assert.ErrorContains(g, err, testCase.expectedErr)
				} else {
					assert.Nil(g, err)
				}
			},
		)
	}
}

func TestDemuxingSenderFromFlagsParseConfig(t *testing.T) {
	testCases := []struct {
		input             string
		expectedDemuxTags []demuxTag
		expectedErr       string
	}{
		{
			// no input
			input: "",
		},
		{
			// simple
			input: "name=/foo:(.+)/,foo",
			expectedDemuxTags: []demuxTag{
				{
					name:        "name",
					regex:       regexp.MustCompile("foo:(.+)"),
					mappedNames: []string{"foo"},
				},
			},
		},
		{
			// simple ends with semicolon
			input: "name=/foo:(.+)/,foo;",
			expectedDemuxTags: []demuxTag{
				{
					name:        "name",
					regex:       regexp.MustCompile("foo:(.+)"),
					mappedNames: []string{"foo"},
				},
			},
		},
		{
			// escaped name
			input: `na\=me=/foo:(.+)/,foo`,
			expectedDemuxTags: []demuxTag{
				{
					name:        "na=me",
					regex:       regexp.MustCompile("foo:(.+)"),
					mappedNames: []string{"foo"},
				},
			},
		},
		{
			// backslashes in name
			input: `na\\me=/foo:(.+)/,foo`,
			expectedDemuxTags: []demuxTag{
				{
					name:        `na\me`,
					regex:       regexp.MustCompile("foo:(.+)"),
					mappedNames: []string{"foo"},
				},
			},
		},
		{
			// escaped mapped name
			input: `na\=me=/foo:(.+)/,foo\,bar`,
			expectedDemuxTags: []demuxTag{
				{
					name:        "na=me",
					regex:       regexp.MustCompile("foo:(.+)"),
					mappedNames: []string{"foo,bar"},
				},
			},
		},
		{
			// two mapped names
			input: "name=/foo:(.+),bar:(.+)/,foo,bar",
			expectedDemuxTags: []demuxTag{
				{
					name:        "name",
					regex:       regexp.MustCompile("foo:(.+),bar:(.+)"),
					mappedNames: []string{"foo", "bar"},
				},
			},
		},
		{
			// alternate regex delimiter
			input: "name=@foo:(.+)/bar:(.+)@,foo,bar",
			expectedDemuxTags: []demuxTag{
				{
					name:        "name",
					regex:       regexp.MustCompile("foo:(.+)/bar:(.+)"),
					mappedNames: []string{"foo", "bar"},
				},
			},
		},
		{
			// escaped regex delimiter
			input: `name=/foo:(.+)\/bar:(.+)/,foo,bar`,
			expectedDemuxTags: []demuxTag{
				{
					name:        "name",
					regex:       regexp.MustCompile("foo:(.+)/bar:(.+)"),
					mappedNames: []string{"foo", "bar"},
				},
			},
		},
		{
			// run-on name
			input:       "name/foo(.+)/,whatever",
			expectedErr: "char 22: unexpected end of transformation",
		},
		{
			// missing regex
			input:       "name=",
			expectedErr: "char 5: unexpected end of transformation",
		},
		{
			// run-on regex
			input:       "name=/foo(.+),whatever",
			expectedErr: "char 22: unexpected end of transformation",
		},
		{
			// missing mapped names
			input:       "name=/foo(.+)/",
			expectedErr: "char 14: unexpected end of transformation",
		},
		{
			// missing comma before mapped names
			input:       "name=/foo(.+)/foo",
			expectedErr: "char 15: expected ','",
		},
		{
			// invalid mapped name
			input:       "name=/foo(.+)/,foo" + string([]byte{0x80, 0x80, 0x80, 0x80, 0x80}),
			expectedErr: "char 18: invalid UTF-8",
		},
		{
			// two mapped names with extra commas
			input:       "name=/foo:(.+),bar:(.+)/,,foo,,bar,,",
			expectedErr: "char 26: expected at least 1 character before ','",
		},
		{
			// bad regex
			input:       "name=/foo/,x",
			expectedErr: `char 0-12: pattern "foo" contains no subexpressions`,
		},
		{
			// multiple transforms
			input: "a=/x:(.+)/,x;b=/y:(.+)/,y",
			expectedDemuxTags: []demuxTag{
				{
					name:        "a",
					regex:       regexp.MustCompile("x:(.+)"),
					mappedNames: []string{"x"},
				},
				{
					name:        "b",
					regex:       regexp.MustCompile("y:(.+)"),
					mappedNames: []string{"y"},
				},
			},
		},
		{
			// multiple transforms with bad regex
			input:       "a=/x:(.+)/,x;BAD=/.../,bad;b=/y:(.+)/,y",
			expectedErr: `char 13-27: pattern "..." contains no subexpressions`,
		},
	}

	for i, testCase := range testCases {
		assert.Group(
			fmt.Sprintf("testCase[%d]: %s", i, testCase.input),
			t,
			func(g *assert.G) {
				ff := &demuxingSenderFromFlags{
					config: testCase.input,
				}

				demuxTags, err := ff.parseConfig()
				assert.ArrayEqual(g, demuxTags, testCase.expectedDemuxTags)
				if testCase.expectedErr != "" {
					assert.ErrorContains(g, err, testCase.expectedErr)
				} else {
					assert.Nil(g, err)
				}
			},
		)

	}
}

func TestDemuxingSenderFromFlagsValidate(t *testing.T) {
	ff := &demuxingSenderFromFlags{}
	assert.Nil(t, ff.Validate())

	ff = &demuxingSenderFromFlags{config: "valid=/a(bc)/,xyz"}
	assert.Nil(t, ff.Validate())

	ff = &demuxingSenderFromFlags{config: "this is invalid"}
	assert.ErrorContains(t, ff.Validate(), "unexpected end of transformation")
}

func TestDemuxingSenderFromFlagsMake(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	underlying := newMockXstatsSender(ctrl)

	ff := &demuxingSenderFromFlags{}
	s, err := ff.Make(underlying, testCleaner)
	assert.SameInstance(t, s, underlying)
	assert.Nil(t, err)

	ff = &demuxingSenderFromFlags{config: "valid=/a(bc)/,xyz"}
	s, err = ff.Make(underlying, testCleaner)
	assert.NotSameInstance(t, s, underlying)
	assert.Nil(t, err)

	ff = &demuxingSenderFromFlags{config: "this is invalid"}
	s, err = ff.Make(underlying, testCleaner)
	assert.Nil(t, s)
	assert.NonNil(t, err)
}
