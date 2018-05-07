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
	tags := []Tag{NewKVTag("a", "b"), NewKVTag(StatusCodeTag, "200"), NewKVTag("x", "y")}

	assert.ArrayEqual(
		t,
		tagNames(statusCodeClassifier(tags)),
		[]string{"a", StatusCodeTag, "x", StatusClassTag},
	)

	tags = []Tag{NewKVTag("a", "b"), NewKVTag(StatusCodeTag, "whoops"), NewKVTag("x", "y")}

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
		{"100", StatusClassSuccess, true},
		{"200", StatusClassSuccess, true},
		{"299", StatusClassSuccess, true},
		{"300", StatusClassRedirect, true},
		{"399", StatusClassRedirect, true},
		{"400", StatusClassClientErr, true},
		{"499", StatusClassClientErr, true},
		{"500", StatusClassServerErr, true},
		{"599", StatusClassServerErr, true},
		{"999", StatusClassServerErr, true},
		{"000", StatusClassServerErr, true},
		{"33", StatusClassServerErr, true},
		{"3", StatusClassServerErr, true},
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
