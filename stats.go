package stats

import (
	"fmt"
	"time"

	"github.com/rs/xstats"
)

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

// Stats is an interface to an underlying stats backend. Tags are ignored
// for backends that do not support them.
type Stats interface {
	// Gauge measures the value of a particular thing at a particular time,
	// like the amount of fuel in a car’s gas tank or the number of users
	// connected to a system.
	Gauge(stat string, value float64, tags ...Tag)

	// Count tracks how many times something happened over a period, like
	// the number of database requests or page views.
	Count(stat string, count float64, tags ...Tag)

	// Histogram tracks the statistical distribution of a set of values,
	// like the duration of a number of database queries or the size of
	// files uploaded by users. The exact measurements tracked vary by
	// backend. For example, statsd will track the average, the minimum,
	// the maximum, the median, the 95th percentile, and the count.
	Histogram(stat string, value float64, tags ...Tag)

	// Timing measures the elapsed time.
	Timing(stat string, value time.Duration, tags ...Tag)

	// AddTag adds a tag to the request client, this tag will be sent with all
	// subsequent stats queries, for backends that support tags.
	AddTags(tags ...Tag)

	// Scope creates a new Stats that appends the given scopes to
	// the prefix for each stat name.
	Scope(scope string, scopes ...string) Stats

	// Close should be called when the Stats is no longer needed
	Close() error
}

func newFromSender(s xstats.Sender, c cleaner, scope string, classifyStatusCodes bool) Stats {
	stats := &xStats{xstats.NewScoping(s, c.scopeDelim), s, c, classifyStatusCodes}
	if scope != "" {
		return stats.Scope(scope)
	}
	return stats
}

type xStats struct {
	xstater             xstats.XStater
	sender              xstats.Sender
	cleaner             cleaner
	classifyStatusCodes bool
}

func (xs *xStats) Gauge(stat string, value float64, tags ...Tag) {
	if xs.classifyStatusCodes {
		tags = statusCodeClassifier(tags)
	}
	xs.xstater.Gauge(xs.cleaner.cleanStatName(stat), value, xs.cleaner.tagsToStrings(tags)...)
}

func (xs *xStats) Count(stat string, count float64, tags ...Tag) {
	if xs.classifyStatusCodes {
		tags = statusCodeClassifier(tags)
	}
	xs.xstater.Count(xs.cleaner.cleanStatName(stat), count, xs.cleaner.tagsToStrings(tags)...)
}

func (xs *xStats) Histogram(stat string, value float64, tags ...Tag) {
	if xs.classifyStatusCodes {
		tags = statusCodeClassifier(tags)
	}
	xs.xstater.Histogram(xs.cleaner.cleanStatName(stat), value, xs.cleaner.tagsToStrings(tags)...)
}

func (xs *xStats) Timing(stat string, value time.Duration, tags ...Tag) {
	if xs.classifyStatusCodes {
		tags = statusCodeClassifier(tags)
	}
	xs.xstater.Timing(xs.cleaner.cleanStatName(stat), value, xs.cleaner.tagsToStrings(tags)...)
}

func (xs *xStats) AddTags(tags ...Tag) {
	if xs.classifyStatusCodes {
		tags = statusCodeClassifier(tags)
	}
	xs.xstater.AddTags(xs.cleaner.tagsToStrings(tags)...)
}

func (xs *xStats) Close() error {
	if err := xstats.CloseSender(xs.sender); err != nil {
		return fmt.Errorf("could not close sender: %s", err)
	}
	return xstats.Close(xs.xstater)
}

func (xs *xStats) Scope(scope string, scopes ...string) Stats {
	xsr := xstats.Scope(xs.xstater, scope, scopes...)
	return &xStats{xsr, xs.sender, xs.cleaner, xs.classifyStatusCodes}
}
