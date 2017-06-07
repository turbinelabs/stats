package stats

import (
	"strings"
	"unicode/utf8"
)

var (
	identity    = func(s string) string { return s }
	strip       = func(_ string) string { return "" }
	stripCommas = mkStrip(",")
	stripColons = mkStrip(":")
)

func mkStrip(set string) func(string) string {
	switch utf8.RuneCountInString(set) {
	case 0:
		return identity
	case 1:
		return func(s string) string { return strings.Replace(s, set, "", -1) }
	case 2:
		r1, next := utf8.DecodeRuneInString(set)
		r2, _ := utf8.DecodeRuneInString(set[next:])

		return func(s string) string {
			return strings.Map(
				func(r rune) rune {
					if r == r1 || r == r2 {
						return -1
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
						return -1
					}
					return r
				},
				s,
			)
		}

	default:
		parts := strings.Split(set, "")
		return func(s string) string {
			for _, part := range parts {
				s = strings.Replace(s, part, "", -1)
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
