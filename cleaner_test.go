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
	"strings"
	"testing"

	"github.com/turbinelabs/test/assert"
)

func toTitle(s string) string {
	if s == "" {
		return s
	}
	return strings.ToTitle(s[0:1]) + strings.ToLower(s[1:])
}

func TestCleanerTagToString(t *testing.T) {
	c := cleaner{
		cleanStatName: strings.ToLower,
		cleanTagName:  strings.ToUpper,
		cleanTagValue: toTitle,
		scopeDelim:    "?",
		tagDelim:      "->",
	}

	testCases := [][]string{
		{"t", "val", "T->Val"},
		{"T", "vAL", "T->Val"},
		{"T", "VAL", "T->Val"},
		{"t", "", "T"},
	}

	for _, tc := range testCases {
		assert.Equal(t, c.tagToString(Tag{K: tc[0], V: tc[1]}), tc[2])
	}

	c = cleaner{
		cleanTagName: strip,
		tagDelim:     ":",
	}

	for _, tc := range testCases {
		assert.Equal(t, c.tagToString(Tag{K: tc[0], V: tc[1]}), "")
	}
}

func TestCleanerTagsToStrings(t *testing.T) {
	c := cleaner{
		cleanStatName: strings.ToLower,
		cleanTagName:  strings.ToUpper,
		cleanTagValue: toTitle,
		scopeDelim:    "?",
		tagDelim:      "->",
	}

	tags := []Tag{
		{K: "t", V: "val"},
		{K: "t"},
		{K: "T2", V: "VAL"},
	}

	assert.ArrayEqual(t, c.tagsToStrings(tags), []string{"T->Val", "T", "T2->Val"})

	c = cleaner{
		cleanTagName: strip,
		tagDelim:     ":",
	}

	assert.ArrayEqual(t, c.tagsToStrings(tags), []string{})
}

func TestMkString(t *testing.T) {
	assert.SameInstance(t, mkStrip(""), identity)

	single := mkStrip("\U0001F622")
	assert.Equal(t, single("\U0001F600\U0001F622"), "\U0001F600")

	double := mkStrip("\U0001F600\U0001F622")
	assert.Equal(t, double("happy\U0001F600 sad\U0001F622"), "happy sad")
	assert.Equal(t, double("ok"), "ok")

	triple := mkStrip("\U0001F600\U0001F622\U0001F60E")
	assert.Equal(
		t,
		triple("happy\U0001F600 sad\U0001F622 dealwithit\U0001F60E"),
		"happy sad dealwithit",
	)
	assert.Equal(t, triple("ok"), "ok")

	quad := mkStrip("\U0001F600\U0001F622\U0001F60E\U0001F635")
	assert.Equal(
		t,
		quad("happy\U0001F600 sad\U0001F622 dealwithit\U0001F60E dead\U0001F635"),
		"happy sad dealwithit dead",
	)
	assert.Equal(t, quad("ok"), "ok")
}

func TestMkReplace(t *testing.T) {
	assert.SameInstance(t, mkReplace("", 'x'), identity)

	single := mkReplace("\U0001F622", 'x')
	assert.Equal(t, single("\U0001F600\U0001F622"), "\U0001F600x")

	double := mkReplace("\U0001F600\U0001F622", 'x')
	assert.Equal(t, double("happy\U0001F600 sad\U0001F622"), "happyx sadx")
	assert.Equal(t, double("ok"), "ok")

	triple := mkReplace("\U0001F600\U0001F622\U0001F60E", 'x')
	assert.Equal(
		t,
		triple("happy\U0001F600 sad\U0001F622 dealwithit\U0001F60E"),
		"happyx sadx dealwithitx",
	)
	assert.Equal(t, triple("ok"), "ok")

	quad := mkReplace("\U0001F600\U0001F622\U0001F60E\U0001F635", 'x')
	assert.Equal(
		t,
		quad("happy\U0001F600 sad\U0001F622 dealwithit\U0001F60E dead\U0001F635"),
		"happyx sadx dealwithitx deadx",
	)
	assert.Equal(t, quad("ok"), "ok")
}

func TestMkExludeFilter(t *testing.T) {
	assert.SameInstance(t, mkExcludeFilter(), identity)

	single := mkExcludeFilter("abc")
	assert.Equal(t, single("abc"), "")
	assert.Equal(t, single("ABC"), "ABC")

	n := mkExcludeFilter("abc", "def", "ghi")
	assert.Equal(t, n("abc"), "")
	assert.Equal(t, n("def"), "")
	assert.Equal(t, n("ghi"), "")
	assert.Equal(t, n("abcdefghi"), "abcdefghi")
}

func TestMkSequence(t *testing.T) {
	assert.SameInstance(t, mkSequence(), identity)
	assert.SameInstance(t, mkSequence(stripColons), stripColons)

	uncalled := func(_ string) string {
		assert.Failed(t, "unexpected call")
		return "FAIL"
	}

	shortCircuit := mkSequence(mkExcludeFilter("abc"), uncalled)
	assert.Equal(t, shortCircuit("abc"), "")

	double := mkSequence(mkExcludeFilter("abc"), stripColons)
	assert.Equal(t, double("a:b:c:"), "abc")

	shortCircuit3 := mkSequence(stripColons, mkExcludeFilter("abc"), uncalled)
	assert.Equal(t, shortCircuit3("a:b:c"), "")

	triple := mkSequence(stripColons, stripCommas, mkExcludeFilter("abc"))
	assert.Equal(t, triple("a,b:c"), "")
	assert.Equal(t, triple("d:e,f"), "def")
}
