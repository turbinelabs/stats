package stats

import (
	"strings"
	"testing"

	"github.com/turbinelabs/test/assert"
)

func TestCleanerTagToString(t *testing.T) {
	c := cleaner{
		cleanTagName:  strings.ToUpper,
		cleanStatName: strings.ToLower,
		scopeDelim:    "?",
		tagDelim:      "->",
	}

	testCases := [][]string{
		{"t", "v", "T->v"},
		{"T", "v", "T->v"},
		{"T", "V", "T->V"},
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
		cleanTagName:  strings.ToUpper,
		cleanStatName: strings.ToLower,
		scopeDelim:    "?",
		tagDelim:      "->",
	}

	tags := []Tag{
		{K: "t", V: "v"},
		{K: "t"},
		{K: "T2", V: "v"},
	}

	assert.ArrayEqual(t, c.tagsToStrings(tags), []string{"T->v", "T", "T2->v"})

	c = cleaner{
		cleanTagName: strip,
		tagDelim:     ":",
	}

	assert.ArrayEqual(t, c.tagsToStrings(tags), []string{})
}
