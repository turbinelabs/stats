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
	cleanTagName:  replaceColonsCommasAndPipes,
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
	w, err := ff.mkUDPWriter()
	if err != nil {
		return nil, err
	}

	underlying := ff.lsff.Make(
		dogstatsd.NewMaxPacket(w, ff.flushInterval, ff.maxPacketLen),
		dogstatsdCleaner,
	)

	return newFromSender(underlying, dogstatsdCleaner, true), nil
}
