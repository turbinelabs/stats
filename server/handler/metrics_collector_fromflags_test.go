package handler

import (
	"errors"
	"flag"
	"log"
	"os"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/turbinelabs/logparser/forwarder"
	"github.com/turbinelabs/test/assert"
)

func TestNewMetricsCollectorFromFlags(t *testing.T) {
	flagset := flag.NewFlagSet("metrics collector options", flag.PanicOnError)

	ff := NewMetricsCollectorFromFlags(flagset)
	ffImpl := ff.(*metricsCollectorFromFlags)

	assert.NonNil(t, ffImpl.forwarderFromFlags)
}

func TestMetricsCollectorFromFlagsValidate(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockForwarderFromFlags := forwarder.NewMockFromFlags(ctrl)

	ff := &metricsCollectorFromFlags{
		forwarderFromFlags: mockForwarderFromFlags,
	}

	validateErr := errors.New("nope")

	gomock.InOrder(
		mockForwarderFromFlags.EXPECT().Validate().Return(nil),
		mockForwarderFromFlags.EXPECT().Validate().Return(validateErr),
	)

	assert.Nil(t, ff.Validate())
	assert.DeepEqual(t, ff.Validate(), validateErr)
}

func TestMetricsCollectorFromFlagsMake(t *testing.T) {
	log := log.New(os.Stderr, "", log.LstdFlags)

	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockForwarderFromFlags := forwarder.NewMockFromFlags(ctrl)
	mockForwarder := forwarder.NewMockForwarder(ctrl)

	ff := &metricsCollectorFromFlags{
		forwarderFromFlags: mockForwarderFromFlags,
	}

	makeErr := errors.New("nope")

	gomock.InOrder(
		mockForwarderFromFlags.EXPECT().Make(log).Return(mockForwarder, nil),
		mockForwarderFromFlags.EXPECT().Make(log).Return(nil, makeErr),
	)

	mc, err := ff.Make(log)
	assert.Nil(t, err)
	mcImpl := mc.(*metricsCollector)
	assert.DeepEqual(t, mcImpl.forwarder, mockForwarder)

	mc, err = ff.Make(log)
	assert.Nil(t, mc)
	assert.DeepEqual(t, err, makeErr)
}
