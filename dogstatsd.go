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
	"github.com/rs/xstats/dogstatsd"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
)

var (
	replaceColonsCommasAndPipes = mkReplace(":|,", '_')
)

// Based on review of data dog's dd-agent (aggregator.py), none of the
// delimiters it uses have escaping mechanisms. Colons are not allowed
// in stat names because they delimit the name from its value. Pipe
// characters delimit the value from metadata. Colons delimit tags
// names from tag values. Commas delimit tags. Strictly speaking,
// multiple colons may appear in a tag, but then it becomes impossible
// to group by values. Thus, none of those characters mays be safely
// use in tag names or tag values.
var dogstatsdCleaner = cleaner{
	cleanStatName: stripColons,
	cleanTagName:  mkSequence(filterTimestamp, replaceColonsCommasAndPipes),
	cleanTagValue: replaceColonsCommasAndPipes,
	tagDelim:      ":",
	scopeDelim:    ".",
}

type dogstatsdFromFlags struct {
	*statsdFromFlags
}

func newDogstatsdFromFlags(fs tbnflag.FlagSet) statsFromFlags {
	return &dogstatsdFromFlags{newStatsdFromFlags(fs)}
}

func (ff *dogstatsdFromFlags) Make() (Stats, error) {
	return ff.makeInternal(dogstatsd.NewMaxPacket, dogstatsdCleaner)
}
