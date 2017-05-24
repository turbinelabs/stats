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
}

func TestCleanerTagsToStrings(t *testing.T) {
	c := cleaner{
		cleanTagName:  strings.ToUpper,
		cleanStatName: strings.ToLower,
		scopeDelim:    "?",
		tagDelim:      "->",
	}

	strs := c.tagsToStrings(
		[]Tag{
			{K: "t", V: "v"},
			{K: "t"},
			{K: "T2", V: "v"},
		},
	)

	assert.ArrayEqual(t, strs, []string{"T->v", "T", "T2->v"})
}
