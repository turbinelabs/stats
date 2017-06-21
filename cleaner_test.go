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
