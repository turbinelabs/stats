package stats

import (
	"fmt"
	"testing"

	"github.com/turbinelabs/test/assert"
)

func TestNewTag(t *testing.T) {
	tag := NewTag("xyz")
	assert.Equal(t, tag, Tag{K: "xyz"})
}

func TestNewKVTag(t *testing.T) {
	tag := NewKVTag("xyz", "pdq")
	assert.Equal(t, tag, Tag{K: "xyz", V: "pdq"})
}

func tagNames(tags []Tag) []string {
	s := make([]string, 0, len(tags))
	for _, tag := range tags {
		s = append(s, tag.K)
	}
	return s
}

func TestStatusCodeClassifier(t *testing.T) {
	tags := []Tag{NewKVTag("a", "b"), NewKVTag(statusCodeTag, "200"), NewKVTag("x", "y")}

	assert.ArrayEqual(
		t,
		tagNames(statusCodeClassifier(tags)),
		[]string{"a", statusCodeTag, "x", statusClassTag},
	)

	tags = []Tag{NewKVTag("a", "b"), NewKVTag(statusCodeTag, "whoops"), NewKVTag("x", "y")}

	assert.ArrayEqual(
		t,
		tagNames(statusCodeClassifier(tags)),
		[]string{"a", "status_code", "x"},
	)

	tags = []Tag{NewKVTag("a", "b"), NewKVTag("blah", "200"), NewKVTag("x", "y")}

	assert.ArrayEqual(
		t,
		tagNames(statusCodeClassifier(tags)),
		[]string{"a", "blah", "x"},
	)
}

func TestStatusClassFromValue(t *testing.T) {
	testCases := []struct {
		statusCode    string
		expectedClass string
		expectedOk    bool
	}{
		{"100", statusClassSuccess, true},
		{"200", statusClassSuccess, true},
		{"299", statusClassSuccess, true},
		{"300", statusClassRedirect, true},
		{"399", statusClassRedirect, true},
		{"400", statusClassClientErr, true},
		{"499", statusClassClientErr, true},
		{"500", statusClassServerErr, true},
		{"599", statusClassServerErr, true},
		{"999", statusClassServerErr, true},
		{"000", statusClassServerErr, true},
		{"33", statusClassServerErr, true},
		{"3", statusClassServerErr, true},
		{"", "", false},
		{"x", "", false},
		{"xx", "", false},
		{"xxx", "", false},
		{"5xx", "", false},
	}

	for _, tc := range testCases {
		assert.Group(
			fmt.Sprintf("Status Code %s", tc.statusCode),
			t,
			func(g *assert.G) {
				statusClass, ok := statusClassFromValue(tc.statusCode)
				assert.Equal(t, statusClass, tc.expectedClass)
				assert.Equal(t, ok, tc.expectedOk)
			},
		)
	}
}
