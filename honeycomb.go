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
	"net/url"

	"github.com/honeycombio/libhoney-go"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/nonstdlib/log/console"
)

type honeycombFromFlags struct {
	flagScope  string
	writeKey   string
	dataset    string
	apiHost    string
	sampleRate uint
	batchSize  uint
	output     libhoney.Output
}

func newHoneycombFromFlags(fs tbnflag.FlagSet) *honeycombFromFlags {
	ff := &honeycombFromFlags{
		flagScope: fs.GetScope(),
	}

	fs.StringVar(
		&ff.writeKey,
		"write-key",
		"",
		"They Honeycomb write key used to send messages.",
	)

	fs.StringVar(
		&ff.dataset,
		"dataset",
		"",
		"They Honeycomb dataset to send messages to.",
	)

	fs.StringVar(
		&ff.apiHost,
		"api-host",
		"https://api.honeycomb.io",
		"The Honeycomb API host to send messages to",
	)

	fs.UintVar(
		&ff.sampleRate,
		"sample-rate",
		1,
		"The Honeycomb sample rate to use. Specified as 1 event sent per Sample Rate",
	)

	fs.UintVar(
		&ff.batchSize,
		"batchSize",
		50,
		"The Honeycomb batch size to use",
	)

	return ff
}

func (ff *honeycombFromFlags) Validate() error {
	if ff.writeKey == "" {
		return fmt.Errorf("must specify a write key")
	}
	if ff.dataset == "" {
		return fmt.Errorf("must specify a dataset")
	}
	if ff.sampleRate <= 0 {
		return fmt.Errorf("sample rate must be greater than zero")
	}
	_, err := url.ParseRequestURI(ff.apiHost)
	if err != nil {
		return fmt.Errorf("must specify a valid api-host: %v", err)
	}
	return nil
}

func (ff *honeycombFromFlags) Make() (Stats, error) {
	honeyConf := libhoney.Config{
		WriteKey:   ff.writeKey,
		Dataset:    ff.dataset,
		APIHost:    ff.apiHost,
		SampleRate: ff.sampleRate,
	}

	if ff.output != nil {
		honeyConf.Output = ff.output
	}

	libhoney.Init(honeyConf)
	return &honeySender{
		builder: libhoney.NewBuilder(),
		tags:    []Tag{},
	}, nil
}

type honeySender struct {
	eventSender
	builder *libhoney.Builder
	tags    []Tag
}

func (hs *honeySender) AddTags(tags ...Tag) {
	hs.tags = append(hs.tags, tags...)
	for _, tag := range tags {
		hs.builder.AddField(tag.K, tag.V)
	}
}

func (hs *honeySender) Event(stat string, fields ...Field) {
	evt := hs.builder.NewEvent()
	evt.AddField("operation", stat)
	for _, field := range fields {
		evt.AddField(field.K, field.V)
	}
	err := evt.Send()
	if err != nil {
		console.Error().Printf("error sending event: %v\n", err)
	} else {
		console.Debug().Println("sent event")
	}
}

func (hs *honeySender) Scope(scope string, scopes ...string) Stats {
	nes := hs.eventSender.scope(scope, scopes...)
	newBuilder := hs.builder.Clone()
	newBuilder.AddField("scopes", nes.scopes)
	return &honeySender{
		eventSender: nes,
		builder:     newBuilder,
		tags:        hs.tags,
	}
}

func (hs *honeySender) Close() error {
	libhoney.Close()
	return nil
}
