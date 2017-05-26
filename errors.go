package stats

import (
	"fmt"
	"strings"
)

func sanitize(r rune) rune {
	switch r {
	case '.':
		return '_'
	case '*':
		return -1
	}
	return r
}

// SanitizeErrorType converts an error's type into a format suitable for
// inclusion in a statsd metric. Specifically it removes '*' and replaces
// '.' with '_'.
func SanitizeErrorType(err error) string {
	return strings.Map(sanitize, fmt.Sprintf("%T", err))
}
