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
	"testing"

	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/turbinelabs/test/assert"
)

func TestHoneycombBackend(t *testing.T) {
	// output is provided by honeycomb for use in tests.
	// it records events in an array.
	honeyOut := &libhoney.MockOutput{}

	hff := &honeycombFromFlags{
		"scope-honeycomb",
		"honeycomb-write-key",
		"honeycomb-dataset",
		"honeycomb-api-host",
		1,
		1,
		honeyOut,
	}

	stats, err := hff.Make()
	assert.Nil(t, err)
	defer stats.Close()

	stats.Event("foo", NewField("hi", "there"))
	assert.Equal(t, len(honeyOut.Events()), 1)
	evt := honeyOut.Events()[0]
	assert.Equal(t, evt.Fields()["operation"], "foo")
	assert.Equal(t, evt.Fields()["hi"], "there")

}

func TestHoneycombScopes(t *testing.T) {
	// output is provided by honeycomb for use in tests.
	// it records events in an array.
	honeyOut := &libhoney.MockOutput{}

	hff := &honeycombFromFlags{
		"scope-honeycomb",
		"honeycomb-write-key",
		"honeycomb-dataset",
		"honeycomb-api-host",
		1,
		1,
		honeyOut,
	}

	stats, err := hff.Make()
	assert.Nil(t, err)
	defer stats.Close()

	stats.Event("event-1", NewField("hi", "there"))
	assert.Equal(t, len(honeyOut.Events()), 1)
	evt := honeyOut.Events()[0]
	assert.Equal(t, evt.Fields()["operation"], "event-1")
	assert.Equal(t, evt.Fields()["hi"], "there")

	scopedStats := stats.Scope("foo", "bar")
	scopedStats.Event("event-2", NewField("hi", "there"))

	evt = honeyOut.Events()[1]
	assert.Equal(t, evt.Fields()["operation"], "event-2")
	assert.Equal(t, evt.Fields()["hi"], "there")
	assert.Equal(t, len(evt.Fields()["scopes"].([]string)), 2)
	assert.Equal(t, evt.Fields()["scopes"].([]string)[0], "foo")
	assert.Equal(t, evt.Fields()["scopes"].([]string)[1], "bar")

}

func TestHoneycombTags(t *testing.T) {
	// output is provided by honeycomb for use in tests.
	// it records events in an array.
	honeyOut := &libhoney.MockOutput{}

	hff := &honeycombFromFlags{
		"scope-honeycomb",
		"honeycomb-write-key",
		"honeycomb-dataset",
		"honeycomb-api-host",
		1,
		1,
		honeyOut,
	}

	stats, err := hff.Make()
	assert.Nil(t, err)
	defer stats.Close()

	stats.Event("event-1", NewField("hi", "there"))
	assert.Equal(t, len(honeyOut.Events()), 1)
	evt := honeyOut.Events()[0]
	assert.Equal(t, evt.Fields()["operation"], "event-1")
	assert.Equal(t, evt.Fields()["hi"], "there")

	stats.AddTags(NewKVTag("tag-1", "v1"), NewTag("tag-2"))
	stats.Event("event-2", NewField("hi", "there"))

	evt = honeyOut.Events()[1]
	assert.Equal(t, evt.Fields()["operation"], "event-2")
	assert.Equal(t, evt.Fields()["hi"], "there")
	assert.Equal(t, evt.Fields()["tag-1"], "v1")
	assert.Equal(t, evt.Fields()["tag-2"], "")
	// ensure nonexistent tag is handled differently than a simple (non-kv) tag
	assert.Equal(t, evt.Fields()["tag-3"], nil)
}
