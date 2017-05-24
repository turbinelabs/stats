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
		cleanTagName:  identity,
		cleanStatName: identity,
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
		cleanTagName:  strip,
		cleanStatName: strip,
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

	s := newFromSender(mockSender, newIdentityCleaner()).Scope("foo").Scope("bar", "baz")
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

	s := newFromSender(mockSender, newIdentityCleaner()).Scope("foo").Scope("bar", "baz")
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

	s := newFromSender(mockSender, newPrefixStrippingCleaner())

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

	s := newFromSender(mockSender, newIdentityCleaner()).Scope("foo").Scope("bar", "baz")
	s.Gauge("gauge", 1.0)
	s.Count("count", 2.0)
	s.Histogram("histo", 3.0)
	s.Timing("time", time.Second)
}

func TestTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockSender := newMockXstatsSender(ctrl)
	mockSender.EXPECT().Gauge("gauge", 1.0, "type=gauge")
	mockSender.EXPECT().Count("count", 2.0, "type=counter")
	mockSender.EXPECT().Histogram("histo", 3.0, "type=histogram")
	mockSender.EXPECT().Timing("time", time.Second, "type=timing")

	s := newFromSender(mockSender, newIdentityCleaner())
	s.Gauge("gauge", 1.0, NewKVTag("type", "gauge"))
	s.Count("count", 2.0, NewKVTag("type", "counter"))
	s.Histogram("histo", 3.0, NewKVTag("type", "histogram"))
	s.Timing("time", time.Second, NewKVTag("type", "timing"))
}

func TestAddTags(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockSender := newMockXstatsSender(ctrl)
	mockSender.EXPECT().Gauge("gauge", 1.0, "type=gauge", "a=b")

	s := newFromSender(mockSender, newIdentityCleaner())
	s.AddTags(NewKVTag("a", "b"))

	s.Gauge("gauge", 1.0, NewKVTag("type", "gauge"))
}
