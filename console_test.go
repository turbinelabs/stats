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
	"bytes"
	"testing"

	"github.com/turbinelabs/test/assert"
)

func TestConsoleBackend(t *testing.T) {
	consoleBuffer := &bytes.Buffer{}
	cff := &consoleFromFlags{
		consoleBuffer,
		"flag-scope",
	}

	stats, err := cff.Make()
	assert.Nil(t, err)
	defer stats.Close()

	stats.Event("foo", NewField("hi", "there"))
	assert.MatchesRegex(t, consoleBuffer.String(), "^.*foo - hi: there\n")
}

func TestConsoleScopes(t *testing.T) {
	consoleBuffer := &bytes.Buffer{}
	cff := &consoleFromFlags{
		consoleBuffer,
		"flag-scope",
	}

	stats, err := cff.Make()
	assert.Nil(t, err)
	defer stats.Close()

	stats.Event("foo", NewField("hi", "there"))
	assert.MatchesRegex(t, consoleBuffer.String(), "^\\S* - foo - hi: there")

	scopedStats := stats.Scope("foo", "bar")
	defer scopedStats.Close()

	consoleBuffer.Reset()
	stats.Event("foo", NewField("hi", "there"))
	assert.MatchesRegex(t, consoleBuffer.String(), "^\\S* - foo - hi: there")

	consoleBuffer.Reset()
	scopedStats.Event("foo", NewField("hi", "there"))
	assert.MatchesRegex(t, consoleBuffer.String(), "^\\S* - foo/bar - foo - hi: there")
}

func TestConsoleTags(t *testing.T) {
	consoleBuffer := &bytes.Buffer{}
	cff := &consoleFromFlags{
		consoleBuffer,
		"flag-scope",
	}

	stats, err := cff.Make()
	assert.Nil(t, err)
	defer stats.Close()

	stats.Event("foo", NewField("hi", "there"))
	assert.MatchesRegex(t, consoleBuffer.String(), "^.\\S* - foo - hi: there")

	stats.AddTags(NewKVTag("tag-1", "v1"), NewTag("tag-2"))
	consoleBuffer.Reset()
	stats.Event("foo", NewField("hi", "there"))
	assert.MatchesRegex(t, consoleBuffer.String(), "^.\\S* - tag-1: v1 - tag-2:  - foo - hi: there")
}
