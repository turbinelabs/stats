package stats

import "strings"

var (
	identity    = func(s string) string { return s }
	strip       = func(_ string) string { return "" }
	stripCommas = func(s string) string { return strings.Replace(s, ",", "_", -1) }
	stripColons = func(s string) string { return strings.Replace(s, ":", "_", -1) }
)

type cleaner struct {
	cleanStatName func(string) string
	cleanTagName  func(string) string
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

	return cleanName + c.tagDelim + tag.V
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
