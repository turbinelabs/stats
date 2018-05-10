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
	"testing"

	"github.com/turbinelabs/test/assert"
)

func TestNewTagTransformer(t *testing.T) {
	testTagTransform, err := newTagTransform("a", "^foo(.*)$", []string{"b"})
	assert.Nil(t, err)

	tt := newTagTransformer([]tagTransform{testTagTransform})
	assert.Equal(t, len(tt.transforms), 1)
	assert.NonNil(t, tt.transforms["a"])
	assert.NotSameInstance(t, tt.transforms["a"], &testTagTransform)

	tt = newTagTransformer([]tagTransform{})
	assert.Equal(t, len(tt.transforms), 0)

	tt = newTagTransformer(nil)
	assert.Equal(t, len(tt.transforms), 0)
}

func TestNewTagTransform(t *testing.T) {
	tt, err := newTagTransform("name", "*bad pattern*", nil)
	assert.DeepEqual(t, tt, tagTransform{})
	assert.ErrorContains(t, err, "error parsing regexp")

	tt, err = newTagTransform("name", "x+", nil)
	assert.DeepEqual(t, tt, tagTransform{})
	assert.ErrorContains(t, err, `pattern "x+" contains no subexpressions`)

	tt, err = newTagTransform("name", "(x+)", nil)
	assert.DeepEqual(t, tt, tagTransform{})
	assert.ErrorContains(t, err, `contains 1 subexpressions, but 0 names were provided`)

	tt, err = newTagTransform("name", "(x+)", []string{"a", "b"})
	assert.DeepEqual(t, tt, tagTransform{})
	assert.ErrorContains(t, err, `contains 1 subexpressions, but 2 names were provided`)

	tt, err = newTagTransform("name", "(x+) (y+)", []string{"a", "b"})
	assert.DeepEqual(t, tt, tagTransform{
		name:            "name",
		regex:           regexp.MustCompile("(x+) (y+)"),
		mappedNames:     []string{"a", "b"},
		replaceOriginal: false,
	})
	assert.Nil(t, err)

	tt, err = newTagTransform("name", "(x+|z*) (y+)", []string{"a", "name"})
	assert.DeepEqual(t, tt, tagTransform{
		name:            "name",
		regex:           regexp.MustCompile("(x+|z*) (y+)"),
		mappedNames:     []string{"a", "name"},
		replaceOriginal: true,
	})
	assert.Nil(t, err)
}

func TestTagTransformTransform(t *testing.T) {
	tt, err := newTagTransform("name", "^foo=(.+),bar=(.*)$", []string{"foo", "bar"})
	assert.Nil(t, err)

	tags, ok := tt.transform(NewKVTag("name", "foo=123,bar=456"))
	assert.ArrayEqual(
		t,
		tags,
		[]Tag{
			NewKVTag("name", "foo=123,bar=456"),
			NewKVTag("foo", "123"),
			NewKVTag("bar", "456"),
		},
	)
	assert.True(t, ok)

	tags, ok = tt.transform(NewKVTag("name", "foo=123,bar="))
	assert.ArrayEqual(
		t,
		tags,
		[]Tag{
			NewKVTag("name", "foo=123,bar="),
			NewKVTag("foo", "123"),
			NewKVTag("bar", ""),
		},
	)
	assert.True(t, ok)

	tags, ok = tt.transform(NewKVTag("name", "foo=123,bar=456"))
	assert.ArrayEqual(
		t,
		tags,
		[]Tag{
			NewKVTag("name", "foo=123,bar=456"),
			NewKVTag("foo", "123"),
			NewKVTag("bar", "456"),
		},
	)
	assert.True(t, ok)

	tags, ok = tt.transform(NewKVTag("name", "foo,bar"))
	assert.Nil(t, tags)
	assert.False(t, ok)

	// Nested subexpressions
	tt, err = newTagTransform(
		"node",
		"^((.+)-(prod|canary|dev)-[a-z0-9]+)-(.+)$",
		[]string{"node", "app", "stage", "version"},
	)
	assert.Nil(t, err)

	tags, ok = tt.transform(NewKVTag("node", "the-app-canary-pod871-2018-05-09-1552a"))
	assert.ArrayEqual(
		t,
		tags,
		[]Tag{
			NewKVTag("node", "the-app-canary-pod871"),
			NewKVTag("app", "the-app"),
			NewKVTag("stage", "canary"),
			NewKVTag("version", "2018-05-09-1552a"),
		},
	)
}

func TestTagTransformTransformWithReplaceOriginal(t *testing.T) {
	tt, err := newTagTransform("x", "^(.+),y=(.*)$", []string{"x", "why"})
	assert.Nil(t, err)

	tags, ok := tt.transform(NewKVTag("x", "123,y=456"))
	assert.ArrayEqual(
		t,
		tags,
		[]Tag{
			NewKVTag("x", "123"),
			NewKVTag("why", "456"),
		},
	)
	assert.True(t, ok)

	tags, ok = tt.transform(NewKVTag("x", "123,y"))
	assert.Nil(t, tags)
	assert.False(t, ok)
}

func TestTagTransformerTransform(t *testing.T) {
	T1 := NewKVTag("a", "b")
	T2 := NewKVTag("c", "d")
	T3 := NewKVTag("e", "f")

	tt1, err := newTagTransform("d1", "x:(.*):y:(.*)", []string{"d1x", "d1y"})
	assert.Nil(t, err)

	tt2, err := newTagTransform("d2x", "(.*):y:(.*)", []string{"d2x", "d2y"})
	assert.Nil(t, err)

	tt := newTagTransformer([]tagTransform{tt1, tt2})

	noMatches := [][]Tag{
		{T1, T2},
		{NewKVTag("d1", "nomatch")},
		{NewKVTag("d2x", "nomatch")},
	}

	for _, tags := range noMatches {
		assert.ArrayEqual(t, tt.transform(tags), tags)
	}

	matches := []struct {
		name         string
		inputTags    []Tag
		expectedTags []Tag
	}{
		{
			name: "just replacements",
			inputTags: []Tag{
				NewKVTag("d1", "x:1:y:2"),
				NewKVTag("d2x", "3:y:4"),
			},
			expectedTags: []Tag{
				NewKVTag("d1", "x:1:y:2"),
				NewKVTag("d1x", "1"),
				NewKVTag("d1y", "2"),
				NewKVTag("d2x", "3"),
				NewKVTag("d2y", "4"),
			},
		},
		{
			name: "replacements and non-matches",
			inputTags: []Tag{T1,
				NewKVTag("d1", "x:1:y:2"),
				T2,
				NewKVTag("d2x", "3:y:4"),
				T3,
			},
			expectedTags: []Tag{
				T1,
				NewKVTag("d1", "x:1:y:2"),
				NewKVTag("d1x", "1"),
				NewKVTag("d1y", "2"),
				T2,
				NewKVTag("d2x", "3"),
				NewKVTag("d2y", "4"),
				T3,
			},
		},
		{
			name: "replacements and non-matches 2",
			inputTags: []Tag{
				NewKVTag("d1", "x:1:y:2"),
				T1,
				NewKVTag("d2x", "3:y:4"),
			},
			expectedTags: []Tag{
				NewKVTag("d1", "x:1:y:2"),
				NewKVTag("d1x", "1"),
				NewKVTag("d1y", "2"),
				T1,
				NewKVTag("d2x", "3"),
				NewKVTag("d2y", "4"),
			},
		},
		{
			name: "just replacements, reverse config order",
			inputTags: []Tag{
				NewKVTag("d2x", "3:y:4"),
				NewKVTag("d1", "x:1:y:2"),
			},
			expectedTags: []Tag{
				NewKVTag("d2x", "3"),
				NewKVTag("d2y", "4"),
				NewKVTag("d1", "x:1:y:2"),
				NewKVTag("d1x", "1"),
				NewKVTag("d1y", "2"),
			},
		},
		{
			name: "replacements and non-matches, reverse config order",
			inputTags: []Tag{
				NewKVTag("d2x", "3:y:4"),
				T1,
				NewKVTag("d1", "x:1:y:2"),
			},
			expectedTags: []Tag{
				NewKVTag("d2x", "3"),
				NewKVTag("d2y", "4"),
				T1,
				NewKVTag("d1", "x:1:y:2"),
				NewKVTag("d1x", "1"),
				NewKVTag("d1y", "2"),
			},
		},
		{
			name: "replacements and duplicate tags",
			inputTags: []Tag{
				NewKVTag("d1", "x:1:y:2"),
				NewKVTag("d2x", "3:y:4"),
				NewKVTag("d1x", "!"),
				NewKVTag("d2y", "!!"),
			},
			expectedTags: []Tag{
				NewKVTag("d1", "x:1:y:2"),
				NewKVTag("d1x", "1"),
				NewKVTag("d1y", "2"),
				NewKVTag("d2x", "3"),
				NewKVTag("d2y", "4"),
				NewKVTag("d1x", "!"),
				NewKVTag("d2y", "!!"),
			},
		},
		{
			name: "replacements and duplicate tags, reverse order",
			inputTags: []Tag{
				NewKVTag("d1x", "!"),
				NewKVTag("d2y", "!!"),
				NewKVTag("d1", "x:1:y:2"),
				NewKVTag("d2x", "3:y:4"),
			},
			expectedTags: []Tag{
				NewKVTag("d1x", "!"),
				NewKVTag("d2y", "!!"),
				NewKVTag("d1", "x:1:y:2"),
				NewKVTag("d1x", "1"),
				NewKVTag("d1y", "2"),
				NewKVTag("d2x", "3"),
				NewKVTag("d2y", "4"),
			},
		},
	}

	for i, match := range matches {
		assert.Group(
			fmt.Sprintf("match[%d]: %s", i, match.name),
			t,
			func(g *assert.G) {
				assert.ArrayEqual(g, tt.transform(match.inputTags), match.expectedTags)
			},
		)
	}
}
