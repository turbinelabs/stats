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

	"github.com/turbinelabs/nonstdlib/arrays/indexof"
)

const (
	transformTagsDesc = `
Defines one or more transformations for tags. A tag with a specific name whose value matches
a regular expression can be transformed into one or more tags with values extracted from
subexpressions of the regular expression. Transformations are specified as follows:

    tag=/regex/,n1,n2...

where tag is the name of the tag to be transformed, regex is a regular expression with 1 or
more subexpressions, and n1,n2... is a sequence of names for the tags formed from the regular
expression's subexpressions (matching groups). Any character may be used in place of the
slashes (/) to delimit the regular expression. There must be at least one subexpression in the
regular expression. There must be exactly as many names as subexpressions. If one of the names
is the original tag name, the original tag is replaced with the transformed value. Otherwise,
the original tag is passed through unchanged. Multiple transformations may be separated by
semicolons (;). Any character may be escaped with a backslash (\).

Examples:
    foo=/^(.+):.*x=([0-9]+)/,foo,bar
    foo=@.*y=([A-Za-z_]+)@,yval
`
)

// newTagTransformer creates a new tagTransformer with the given underlying
// tagTransforms.
func newTagTransformer(config []tagTransform) *tagTransformer {
	if len(config) == 0 {
		return &tagTransformer{}
	}

	m := make(map[string]*tagTransform, len(config))
	for _, tt := range config {
		ttCopy := tt
		m[ttCopy.name] = &ttCopy
	}

	return &tagTransformer{transforms: m}
}

// newTagTransform creates a new tagTransform from the given tag name, value pattern,
// and mapped tag names. The pattern must be a valid regular expression with at least
// one subexpression. There must be exactly one mapped name for each subexpression in
// the pattern. If a mapped name equals the original tag name, the original tag will
// be replaced with the subexpression's value. Otherwise, the original tag is left in
// place.
func newTagTransform(name, pattern string, mappedNames []string) (tagTransform, error) {
	r, err := regexp.Compile(pattern)
	if err != nil {
		return tagTransform{}, err
	}

	if r.NumSubexp() == 0 {
		return tagTransform{}, fmt.Errorf("pattern %q contains no subexpressions", pattern)
	}

	if r.NumSubexp() != len(mappedNames) {
		return tagTransform{},
			fmt.Errorf(
				"pattern %q contains %d subexpressions, but %d names were provided",
				pattern,
				r.NumSubexp(),
				len(mappedNames),
			)
	}

	return tagTransform{
		name:            name,
		regex:           r,
		mappedNames:     mappedNames,
		replaceOriginal: indexof.String(mappedNames, name) != indexof.NotFound,
	}, nil
}

// tagTransformer provides a mechanism to allow a single tag value to be broken up
// into multiple component tags. For example, ENCHILADA=chicken,corn,cotija can be
// transformed into FILLING=chicken, TORTILLA=corn, CHEESE=cotija.
type tagTransformer struct {
	transforms map[string]*tagTransform
}

// tagTransform represents a single tag transform.
type tagTransform struct {
	name            string
	regex           *regexp.Regexp
	mappedNames     []string
	replaceOriginal bool
}

func (dt *tagTransform) transform(original Tag) ([]Tag, bool) {
	submatches := dt.regex.FindStringSubmatch(original.V)
	if len(submatches) < 2 {
		return nil, false
	}

	var tags []Tag
	if dt.replaceOriginal {
		tags = make([]Tag, 0, len(submatches)-1)
	} else {
		tags = append(make([]Tag, 0, len(submatches)), original)
	}

	for i, submatch := range submatches[1:] {
		t := NewKVTag(dt.mappedNames[i], submatch)
		tags = append(tags, t)
	}

	return tags, true
}

func (t *tagTransformer) transform(tags []Tag) []Tag {
	if len(t.transforms) == 0 {
		return tags
	}

	ttags := map[int][]Tag{}
	numReplacements := 0
	for i, tag := range tags {
		if tt, ok := t.transforms[tag.K]; ok {
			if replacements, ok := tt.transform(tag); ok {
				ttags[i] = replacements
				numReplacements += len(replacements)
			}
		}
	}

	if numReplacements == 0 {
		// No matches
		return tags
	}

	// Capacity is number of original tags, minus those for which replacement will
	// occur, plus the number of replacement tags.
	rtags := make([]Tag, 0, len(tags)-len(ttags)+numReplacements)
	for i, tag := range tags {
		if replacements, ok := ttags[i]; ok {
			rtags = append(rtags, replacements...)
		} else {
			rtags = append(rtags, tag)
		}
	}

	return rtags
}
