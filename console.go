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
	"os"
	"strings"
	"time"

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
)

type consoleFromFlags struct {
	writer    io.Writer
	flagScope string
}

func newConsoleFromFlags(fs tbnflag.FlagSet) *consoleFromFlags {
	ff := &consoleFromFlags{
		writer:    os.Stdout,
		flagScope: fs.GetScope(),
	}
	return ff
}

func (ff *consoleFromFlags) Validate() error {
	return nil
}

func (ff *consoleFromFlags) Make() (Stats, error) {
	return &consoleSender{
		writer: ff.writer,
		tags:   []Tag{},
	}, nil
}

type consoleSender struct {
	eventSender
	writer io.Writer
	tags   []Tag
}

func (cs *consoleSender) AddTags(tags ...Tag) {
	cs.tags = append(cs.tags, tags...)
}

func (cs *consoleSender) Event(stat string, fields ...Field) {
	now := time.Now()
	consoleFields := []string{}
	for _, tag := range cs.tags {
		consoleFields = append(consoleFields, fmt.Sprintf("%v: %v", tag.K, tag.V))
	}
	consoleFields = append(consoleFields, stat)
	for _, field := range fields {
		consoleFields = append(consoleFields, fmt.Sprintf("%v: %v", field.K, field.V))
	}
	consoleLine := ""
	if len(cs.eventSender.scopes) > 0 {
		consoleLine = fmt.Sprintf("%v - %v - %v\n", now.Format(time.RFC3339),
			strings.Join(cs.eventSender.scopes, "/"),
			strings.Join(consoleFields, " - "))
	} else {
		consoleLine = fmt.Sprintf("%v - %v\n", now.Format(time.RFC3339), strings.Join(consoleFields, " - "))
	}
	cs.writer.Write([]byte(consoleLine))
}

func (cs *consoleSender) Scope(scope string, scopes ...string) Stats {
	return &consoleSender{
		eventSender: cs.eventSender.scope(scope, scopes...),
		writer:      cs.writer,
		tags:        cs.tags,
	}
}

func (cs *consoleSender) Close() error {
	return nil
}
