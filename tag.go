package stats

// Tag is an optional piece of metadata to be added to one or more stat points
type Tag struct {
	K string
	V string
}

// NewTag produces a new tag from a string
func NewTag(tag string) Tag {
	return Tag{K: tag}
}

// NewKVTag produces a new tag from a key/value pair
func NewKVTag(k, v string) Tag {
	return Tag{k, v}
}
