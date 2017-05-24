package stats

var (
	identity = func(s string) string { return s }
)

type cleaner struct {
	cleanTagName  func(string) string
	cleanStatName func(string) string
	scopeDelim    string
	tagDelim      string
}

func (c cleaner) tagToString(tag Tag) string {
	if tag.V == "" {
		return c.cleanTagName(tag.K)
	}
	return c.cleanTagName(tag.K) + c.tagDelim + tag.V
}

func (c cleaner) tagsToStrings(tags []Tag) []string {
	strs := make([]string, 0, len(tags))
	for _, tag := range tags {
		strs = append(strs, c.tagToString(tag))
	}
	return strs
}
