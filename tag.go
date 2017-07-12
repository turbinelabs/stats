package stats

const (
	StatusCodeTag        = "status_code"
	StatusClassTag       = "status_class"
	StatusClassSuccess   = "success"
	StatusClassRedirect  = "redirect"
	StatusClassClientErr = "client_error"
	StatusClassServerErr = "server_error"

	NodeTag      = "node"
	SourceTag    = "source"
	TimestampTag = "timestamp"
)

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

func statusCodeClassifier(tags []Tag) []Tag {
	for _, tag := range tags {
		if tag.K == StatusCodeTag {
			if statusClass, ok := statusClassFromValue(tag.V); ok {
				return append(tags, NewKVTag(StatusClassTag, statusClass))
			}
			return tags
		}
	}

	return tags
}

func statusClassFromValue(v string) (string, bool) {
	if v == "" {
		return "", false
	}

	// validate numeric
	for _, r := range v {
		if r < '0' || r > '9' {
			return "", false
		}
	}

	if len(v) != 3 {
		return StatusClassServerErr, true
	}

	switch v[0] {
	case '1', '2':
		return StatusClassSuccess, true
	case '3':
		return StatusClassRedirect, true
	case '4':
		return StatusClassClientErr, true
	default:
		return StatusClassServerErr, true
	}
}
