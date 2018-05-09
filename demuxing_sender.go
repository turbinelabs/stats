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
	"time"

	"github.com/turbinelabs/nonstdlib/arrays/indexof"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
)

// newDemuxingSender creates a new demuxingSender with the given underlying
// xstatsSender, cleaner, and demuxTags.
func newDemuxingSender(underlying xstatsSender, c cleaner, config []demuxTag) xstatsSender {
	if len(config) == 0 {
		return underlying
	}

	m := make(map[string]*demuxTag, len(config))
	for _, dt := range config {
		dtCopy := dt
		m[dtCopy.name] = &dtCopy
	}

	return &demuxingSender{underlying: underlying, cleaner: c, tags: m}
}

// newDemuxTag creates a new demuxTag from the given tag name, value pattern, and
// mapped tag names. The pattern must be a valid regular expression with at least one
// subexpression. There must be exactly one mapped name for each subexpression in the
// pattern. If a mapped name equals the original tag name, the original tag will be
// replaced with the subexpression's value. Otherwise, the original tag is left in
// place.
func newDemuxTag(name, pattern string, mappedNames []string) (demuxTag, error) {
	r, err := regexp.Compile(pattern)
	if err != nil {
		return demuxTag{}, err
	}

	if r.NumSubexp() == 0 {
		return demuxTag{}, fmt.Errorf("pattern %q contains no subexpressions", pattern)
	}

	if r.NumSubexp() != len(mappedNames) {
		return demuxTag{},
			fmt.Errorf(
				"pattern %q contains %d subexpressions, but %d names were provided",
				pattern,
				r.NumSubexp(),
				len(mappedNames),
			)
	}

	return demuxTag{
		name:            name,
		regex:           r,
		mappedNames:     mappedNames,
		replaceOriginal: indexof.String(mappedNames, name) != indexof.NotFound,
	}, nil
}

// demuxingSender extends xtsats.Sender to provide a tag demultiplexing step that
// allows a single tag value to be broken up into multiple component tags. For
// example, ENCHILADA=chicken,corn,cotija can be demultiplexed into FILLING=chicken,
// TORTILLA=corn, CHEESE=cotija.
type demuxingSender struct {
	underlying xstatsSender
	cleaner    cleaner
	tags       map[string]*demuxTag
}

// demuxTag represents a tag to demultiplex.
type demuxTag struct {
	name            string
	regex           *regexp.Regexp
	mappedNames     []string
	replaceOriginal bool
}

func (dt *demuxTag) demux(original, value, tagDelim string) ([]string, bool) {
	submatches := dt.regex.FindStringSubmatch(value)
	if len(submatches) < 2 {
		return nil, false
	}

	var tags []string
	if dt.replaceOriginal {
		tags = make([]string, 0, len(submatches)-1)
	} else {
		tags = append(make([]string, 0, len(submatches)), original)
	}

	for i, submatch := range submatches[1:] {
		t := fmt.Sprintf("%s%s%s", dt.mappedNames[i], tagDelim, submatch)
		tags = append(tags, t)
	}

	return tags, true
}

func (s *demuxingSender) demuxTags(tags []string) []string {
	dtags := map[int][]string{}

	numReplacements := 0
	for i, tag := range tags {
		name, value := tbnstrings.Split2(tag, s.cleaner.tagDelim)

		if dt, ok := s.tags[name]; ok {
			if replacements, ok := dt.demux(tag, value, s.cleaner.tagDelim); ok {
				dtags[i] = replacements
				numReplacements += len(replacements)
			}
		}
	}

	if numReplacements == 0 {
		// No matches
		return tags
	}

	// capacity is number of original tags, minus those for which replacement will
	// occur, plus the number of replacement tags. (Note that the demuxTag.demux tag
	// call will return the original tag if configured to do so.)
	rtags := make([]string, 0, len(tags)-len(dtags)+numReplacements)
	for i, tag := range tags {
		if replacements, ok := dtags[i]; ok {
			rtags = append(rtags, replacements...)
		} else {
			rtags = append(rtags, tag)
		}
	}
	return rtags
}

func (s *demuxingSender) Count(stat string, count float64, tags ...string) {
	s.underlying.Count(stat, count, s.demuxTags(tags)...)
}

func (s *demuxingSender) Gauge(stat string, value float64, tags ...string) {
	s.underlying.Gauge(stat, value, s.demuxTags(tags)...)
}

func (s *demuxingSender) Histogram(stat string, value float64, tags ...string) {
	s.underlying.Histogram(stat, value, s.demuxTags(tags)...)
}

func (s *demuxingSender) Timing(stat string, value time.Duration, tags ...string) {
	s.underlying.Timing(stat, value, s.demuxTags(tags)...)
}
