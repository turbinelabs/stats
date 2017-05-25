package stats

import (
	"github.com/rs/xstats/dogstatsd"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
)

var dogStatsdCleaner = cleaner{
	cleanStatName: stripColons,
	cleanTagName:  stripCommas,
	tagDelim:      ":",
	scopeDelim:    ".",
}

type dogstatsdFromFlags struct {
	*statsdFromFlags
}

func newDogstatsdFromFlags(fs tbnflag.FlagSet) statsFromFlags {
	return &dogstatsdFromFlags{newStatsdFromFlags(fs, dogstatsdName)}
}

func (ff *dogstatsdFromFlags) Make() (Stats, error) {
	w, err := ff.mkUDPWriter()
	if err != nil {
		return nil, err
	}
	return newFromSender(dogstatsd.New(w, ff.flushInterval), dogStatsdCleaner), nil
}
