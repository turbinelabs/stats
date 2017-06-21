package stats

import (
	"fmt"
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
	return mkReplace(set, -1)
}

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
