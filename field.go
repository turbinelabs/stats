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

import "fmt"

// A field is a more general variant of Tag. Instead of a string key and a
// string value, Tag supports an arbitrary interface for the value. Note that it
// is used when sending structured events, so the value should be serializable
// as JSON.
type Field struct {
	K string
	V interface{}
}

// NewField produces a new tag from a string
func NewField(key string, val interface{}) Field {
	return Field{key, val}
}

// Creates a Field from a Tag object, preserving
// the key and value
func FieldFromTag(tag Tag) Field {
	return Field{tag.K, tag.V}
}

// Creates an array of Fields from an array of Tags
func FieldsFromTags(tags []Tag) []Field {
	fields := make([]Field, len(tags))
	for i, t := range tags {
		fields[i] = FieldFromTag(t)
	}
	return fields
}

// Creates an array of fields from a mop of strings to interface{}.
// Each Field in the new array will have a key in the format
// '<prefix>.<key from map>', with the value being the value from
// the supplied map
func FieldsFromMap(prefix string, m map[string]string) []Field {
	i := 0
	fields := make([]Field, len(m))
	for k, v := range m {
		fields[i] = NewField(fmt.Sprintf("%v.%v", prefix, k), v)
		i++
	}
	return fields
}
