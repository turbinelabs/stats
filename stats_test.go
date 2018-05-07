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
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/rs/xstats"

	"github.com/turbinelabs/test/assert"
)

func newIdentityCleaner() cleaner {
	return cleaner{
		cleanStatName: identity,
		cleanTagName:  identity,
		cleanTagValue: identity,
		scopeDelim:    ".",
		tagDelim:      "=",
	}
}

func newPrefixStrippingCleaner() cleaner {
	strip := func(s string) string {
		idx := strings.Index(s, ":")
		if idx >= 0 {
			return s[idx+1:]
		}
		return s
	}

	return cleaner{
		cleanStatName: strip,
		cleanTagName:  strip,
		cleanTagValue: strip,
		scopeDelim:    ".",
		tagDelim:      "=",
	}

}

type closeableSender struct {
	xstats.Sender
	closeErr  error
	wasClosed bool
}

func (cs *closeableSender) Close() error {
	cs.wasClosed = true
	return cs.closeErr
}

var _ io.Closer = &closeableSender{}

func TestClose(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()
	mockSender := &closeableSender{Sender: newMockXstatsSender(ctrl)}

	s := newFromSender(mockSender, newIdentityCleaner(), "", false).Scope("foo").Scope("bar", "baz")
	assert.Nil(t, s.Close())
	assert.True(t, mockSender.wasClosed)
}

func TestCloseErr(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockSender := &closeableSender{
		Sender:   newMockXstatsSender(ctrl),
		closeErr: errors.New("gah"),
	}

	s := newFromSender(mockSender, newIdentityCleaner(), "", false).Scope("foo").Scope("bar", "baz")
	assert.ErrorContains(t, s.Close(), "could not close sender: gah")
	assert.True(t, mockSender.wasClosed)
}

func TestTypes(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockSender := newMockXstatsSender(ctrl)
	gomock.InOrder(
		mockSender.EXPECT().Gauge("g", 1.0),
		mockSender.EXPECT().Count("c", 2.0),
		mockSender.EXPECT().Histogram("h", 3.0),
		mockSender.EXPECT().Timing("t", time.Second),
	)

	s := newFromSender(mockSender, newPrefixStrippingCleaner(), "", false)

	s.Gauge("abc:g", 1.0)
	s.Count("def:c", 2.0)
	s.Histogram("ghi:h", 3.0)
	s.Timing("jkl:t", time.Second)
}

func TestScope(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockSender := newMockXstatsSender(ctrl)
	mockSender.EXPECT().Gauge("foo.bar.baz.gauge", 1.0)
	mockSender.EXPECT().Count("foo.bar.baz.count", 2.0)
	mockSender.EXPECT().Histogram("foo.bar.baz.histo", 3.0)
	mockSender.EXPECT().Timing("foo.bar.baz.time", time.Second)

	s := newFromSender(mockSender, newIdentityCleaner(), "", false).Scope("foo").Scope("bar", "baz")
	s.Gauge("gauge", 1.0)
	s.Count("count", 2.0)
	s.Histogram("histo", 3.0)
	s.Timing("time", time.Second)

	mockSender.EXPECT().Gauge(
		"foo.bar.baz.gauge",
		1.0,
		StatusCodeTag+"=200",
		StatusClassTag+"="+StatusClassSuccess,
	)
	s = newFromSender(mockSender, newIdentityCleaner(), "foo", true).Scope("bar", "baz")
	s.Gauge("gauge", 1.0, NewKVTag(StatusCodeTag, "200"))
}

func TestTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	codeTag := NewKVTag(StatusCodeTag, "200")
	cleanCodeTag := StatusCodeTag + "=200"
	cleanClassTag := StatusClassTag + "=" + StatusClassSuccess

	mockSender := newMockXstatsSender(ctrl)
	mockSender.EXPECT().Gauge("gauge", 1.0, "type=gauge", cleanCodeTag)
	mockSender.EXPECT().Count("count", 2.0, "type=counter", cleanCodeTag)
	mockSender.EXPECT().Histogram("histo", 3.0, "type=histogram", cleanCodeTag)
	mockSender.EXPECT().Timing("time", time.Second, "type=timing", cleanCodeTag)

	s := newFromSender(mockSender, newIdentityCleaner(), "", false)
	s.Gauge("gauge", 1.0, NewKVTag("type", "gauge"), codeTag)
	s.Count("count", 2.0, NewKVTag("type", "counter"), codeTag)
	s.Histogram("histo", 3.0, NewKVTag("type", "histogram"), codeTag)
	s.Timing("time", time.Second, NewKVTag("type", "timing"), codeTag)

	mockSender.EXPECT().Gauge("gauge", 1.0, "type=gauge", cleanCodeTag, cleanClassTag)
	mockSender.EXPECT().Count("count", 2.0, "type=counter", cleanCodeTag, cleanClassTag)
	mockSender.EXPECT().Histogram("histo", 3.0, "type=histogram", cleanCodeTag, cleanClassTag)
	mockSender.EXPECT().Timing("time", time.Second, "type=timing", cleanCodeTag, cleanClassTag)

	s = newFromSender(mockSender, newIdentityCleaner(), "", true)
	s.Gauge("gauge", 1.0, NewKVTag("type", "gauge"), codeTag)
	s.Count("count", 2.0, NewKVTag("type", "counter"), codeTag)
	s.Histogram("histo", 3.0, NewKVTag("type", "histogram"), codeTag)
	s.Timing("time", time.Second, NewKVTag("type", "timing"), codeTag)
}

func TestAddTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockSender := newMockXstatsSender(ctrl)
	mockSender.EXPECT().Gauge("gauge", 1.0, "type=gauge", StatusCodeTag+"=200")

	s := newFromSender(mockSender, newIdentityCleaner(), "", false)
	s.AddTags(NewKVTag(StatusCodeTag, "200"))

	s.Gauge("gauge", 1.0, NewKVTag("type", "gauge"))

	mockSender.EXPECT().Gauge(
		"gauge",
		1.0,
		"type=gauge",
		StatusCodeTag+"=200",
		StatusClassTag+"="+StatusClassSuccess,
	)

	s = newFromSender(mockSender, newIdentityCleaner(), "", true)
	s.AddTags(NewKVTag(StatusCodeTag, "200"))

	s.Gauge("gauge", 1.0, NewKVTag("type", "gauge"))
}
