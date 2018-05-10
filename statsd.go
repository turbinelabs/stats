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
	"io"
	"net"
	"os"
	"time"

	"github.com/rs/xstats"
	"github.com/rs/xstats/statsd"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
)

const (
	defaultHost          = "127.0.0.1"
	defaultPort          = 8125
	defaultFlushInterval = 5 * time.Second
	defaultMaxPacketLen  = 8192 // assume jumbo ethernet frames that handle 8k payload
)

var statsdCleaner = cleaner{
	cleanStatName: stripColons,
	cleanTagName:  strip,
	cleanTagValue: strip,
	scopeDelim:    ".",
}

var stdoutWriter io.Writer = os.Stdout

type statsdFromFlags struct {
	flagScope     string
	host          string
	port          int
	maxPacketLen  int
	flushInterval time.Duration
	scope         string
	transforms    string
	lsff          *latchingSenderFromFlags
	debug         bool
}

// mkStatsdSenderFunc allows alternate statsd-look-alike APIs to reuse statsdFromFlags.
type mkStatsdSenderFunc func(
	netWriter io.Writer,
	flushInterval time.Duration,
	maxPacketLen int,
) xstats.Sender

func newStatsdFromFlags(fs tbnflag.FlagSet) *statsdFromFlags {
	ff := &statsdFromFlags{
		flagScope: fs.GetScope(),
		lsff:      newLatchingSenderFromFlags(fs, false),
	}

	fs.StringVar(
		&ff.host,
		"host",
		defaultHost,
		"Specifies the destination host for stats.",
	)

	fs.IntVar(
		&ff.port,
		"port",
		defaultPort,
		"Specifies the destination port for stats.",
	)

	fs.IntVar(
		&ff.maxPacketLen,
		"max-packet-len",
		defaultMaxPacketLen,
		"Specifies the maximum number of payload `bytes` sent per flush. If necessary, flushes will occur before the flush interval to prevent payloads from exceeding this size. The size does not include IP and UDP header bytes. Stats may not be delivered if the total size of the headers and payload exceeds the network's MTU.",
	)

	fs.DurationVar(
		&ff.flushInterval,
		"flush-interval",
		defaultFlushInterval,
		"Specifies the `duration` between stats flushes.",
	)

	fs.StringVar(
		&ff.scope,
		"scope",
		"",
		"If specified, prepends the given scope to metric names.",
	)

	fs.StringVar(
		&ff.transforms,
		"transform-tags",
		"",
		transformTagsDesc,
	)

	fs.BoolVar(
		&ff.debug,
		"debug",
		false,
		"If enabled, logs the stats data on stdout.",
	)

	return ff
}

func (ff *statsdFromFlags) Validate() error {
	addr := fmt.Sprintf("%s:%d", ff.host, ff.port)

	if _, _, err := tbnstrings.SplitHostPort(addr); err != nil {
		return fmt.Errorf(
			"--%shost or --%sport is invalid: %s",
			ff.flagScope,
			ff.flagScope,
			err.Error(),
		)
	}

	if ff.flushInterval <= 0*time.Second {
		return fmt.Errorf("--%sflush-interval must be greater than zero", ff.flagScope)
	}

	if _, err := parseTagTransforms(ff.transforms); err != nil {
		return fmt.Errorf("--%stransform-tags invalid: %s", ff.flagScope, err.Error())
	}

	return ff.lsff.Validate()
}

func (ff *statsdFromFlags) Make() (Stats, error) {
	return ff.makeInternal(statsd.NewMaxPacket, statsdCleaner)
}

func (ff *statsdFromFlags) makeInternal(mkSender mkStatsdSenderFunc, c cleaner) (Stats, error) {
	tagTransformer, err := parseTagTransforms(ff.transforms)
	if err != nil {
		return nil, err
	}

	w, err := ff.mkUDPWriter()
	if err != nil {
		return nil, err
	}

	underlying := mkSender(w, ff.flushInterval, ff.maxPacketLen)

	// If latching is disabled, underlying is returned unchanged.
	underlying = ff.lsff.Make(underlying, c)

	return newFromSender(underlying, c, ff.scope, tagTransformer, true), nil
}

func (ff *statsdFromFlags) mkUDPWriter() (io.Writer, error) {
	var (
		w   io.Writer
		err error
	)

	addr := fmt.Sprintf("%s:%d", ff.host, ff.port)
	w, err = net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}

	if ff.debug {
		w = &debugWriter{w, stdoutWriter}
	}

	return w, nil
}

// debugWriter differs from io.MultiWriter in that it ignores short
// writes and errors on its debug Writer.
type debugWriter struct {
	underlying, debug io.Writer
}

func (dw *debugWriter) Write(b []byte) (int, error) {
	defer dw.debug.Write(b)
	return dw.underlying.Write(b)
}
