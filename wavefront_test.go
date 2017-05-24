package stats

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/turbinelabs/test/assert"
)

func TestWavefrontSenderGauge(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()
	mockSender := newMockXstatsSender(ctrl)
	wfs := &wavefrontSender{mockSender}
	mockSender.EXPECT().Gauge("foo~bar=baz~blar=blaz", float64(1234))
	wfs.Gauge("foo", 1234, "bar=baz", "blar=blaz")
}

func TestWavefrontSenderCount(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()
	mockSender := newMockXstatsSender(ctrl)
	wfs := &wavefrontSender{mockSender}
	mockSender.EXPECT().Count("foo~bar=baz~blar=blaz", float64(1234))
	wfs.Count("foo", 1234, "bar=baz", "blar=blaz")
}

func TestWavefrontSenderHistogram(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()
	mockSender := newMockXstatsSender(ctrl)
	wfs := &wavefrontSender{mockSender}
	mockSender.EXPECT().Histogram("foo~bar=baz~blar=blaz", float64(1234))
	wfs.Histogram("foo", 1234, "bar=baz", "blar=blaz")
}

func TestWavefrontSenderTiming(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()
	mockSender := newMockXstatsSender(ctrl)
	wfs := &wavefrontSender{mockSender}
	mockSender.EXPECT().Timing("foo~bar=baz~blar=blaz", 1234*time.Millisecond)
	wfs.Timing("foo", 1234*time.Millisecond, "bar=baz", "blar=blaz")
}
