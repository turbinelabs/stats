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
	"strings"
	"unicode/utf8"
)

var (
	identity        = func(s string) string { return s }
	strip           = func(_ string) string { return "" }
	stripCommas     = mkStrip(",")
	stripColons     = mkStrip(":")
	filterTimestamp = mkExcludeFilter(TimestampTag)
)

// mkStrip creates a function that removes any rune that appears in
// set. See mkReplace.
func mkStrip(set string) func(string) string {
	return mkReplace(set, -1)
}

// mkReplace creates a function that replaces any rune that appears in
// set with the replacement rune. If replacement is -1, the runes are
// removed. If set is empty, identity is returned. The returned
// function is optimized to run in O(n) time, where n is the input
// string length, when set contains fewer than 4 runes. For 4 or more
// runes, it operates in O(n*m) time, where m is the number of runes
// in set.
func mkReplace(set string, replacement rune) func(string) string {
	switch utf8.RuneCountInString(set) {
	case 0:
		return identity
	case 1:
		r := ""
		if replacement != -1 {
			r = fmt.Sprintf("%c", replacement)
		}
		return func(s string) string { return strings.Replace(s, set, r, -1) }
	case 2:
		r1, next := utf8.DecodeRuneInString(set)
		r2, _ := utf8.DecodeRuneInString(set[next:])

		return func(s string) string {
			return strings.Map(
				func(r rune) rune {
					if r == r1 || r == r2 {
						return replacement
					}
					return r
				},
				s,
			)
		}
	case 3:
		r1, next1 := utf8.DecodeRuneInString(set)
		r2, next2 := utf8.DecodeRuneInString(set[next1:])
		r3, _ := utf8.DecodeRuneInString(set[next1+next2:])

		return func(s string) string {
			return strings.Map(
				func(r rune) rune {
					if r == r1 || r == r2 || r == r3 {
						return replacement
					}
					return r
				},
				s,
			)
		}

	default:
		r := ""
		if replacement != -1 {
			r = fmt.Sprintf("%c", replacement)
		}
		parts := strings.Split(set, "")
		return func(s string) string {
			for _, part := range parts {
				s = strings.Replace(s, part, r, -1)
			}
			return s
		}
	}
}

// mkExcludeFilter creates a function returns an empty string when its
// input matches a member of exclusions exactly. If exclusions is
// empty, identity is returned.
func mkExcludeFilter(exclusions ...string) func(string) string {
	switch len(exclusions) {
	case 0:
		return identity
	case 1:
		exclusion := exclusions[0]
		return func(s string) string {
			if s == exclusion {
				return ""
			}
			return s
		}
	default:
		return func(s string) string {
			for _, e := range exclusions {
				if s == e {
					return ""
				}
			}
			return s
		}
	}
}

// mkSequence combines multiple cleaner functions in sequence. It
// terminates immediately if the result of one the functions is the
// empty string. Optimized versions are returned for 0, 1, or 2
// functions.
func mkSequence(fs ...func(string) string) func(string) string {
	switch len(fs) {
	case 0:
		return identity
	case 1:
		return fs[0]
	case 2:
		f1, f2 := fs[0], fs[1]
		return func(s string) string {
			s = f1(s)
			if s != "" {
				s = f2(s)
			}
			return s
		}
	default:
		return func(s string) string {
			for _, f := range fs {
				s = f(s)
				if s == "" {
					break
				}
			}
			return s
		}
	}
}

type cleaner struct {
	cleanStatName func(string) string
	cleanTagName  func(string) string
	cleanTagValue func(string) string
	tagDelim      string
	scopeDelim    string
}

func (c cleaner) tagToString(tag Tag) string {
	cleanName := c.cleanTagName(tag.K)
	if cleanName == "" {
		return ""
	}

	if tag.V == "" {
		return cleanName
	}

	return cleanName + c.tagDelim + c.cleanTagValue(tag.V)
}

func (c cleaner) tagsToStrings(tags []Tag) []string {
	strs := make([]string, 0, len(tags))
	for _, tag := range tags {
		cleanTag := c.tagToString(tag)
		if cleanTag != "" {
			strs = append(strs, cleanTag)
		}
	}
	return strs
}
