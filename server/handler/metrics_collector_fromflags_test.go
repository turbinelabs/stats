package handler

import (
	"errors"
	"flag"
	"log"
	"os"
	"reflect"
	"sync"
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
		bufferSize:         0,
	}

	validateErr := errors.New("nope")

	mockForwarderFromFlags.EXPECT().Validate().Return(nil)
	assert.Nil(t, ff.Validate())

	ff.bufferSize = -1
	assert.ErrorContains(t, ff.Validate(), "buffer-size must not be negative")

	ff.bufferSize = 100
	mockForwarderFromFlags.EXPECT().Validate().Return(validateErr)
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

func TestMetricsCollectorFromFlagsMakeWithNonZeroBufferSize(t *testing.T) {
	log := log.New(os.Stderr, "", log.LstdFlags)

	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	mockForwarderFromFlags := forwarder.NewMockFromFlags(ctrl)
	mockForwarder := forwarder.NewMockForwarder(ctrl)

	mockForwarderFromFlags.EXPECT().Make(log).Return(mockForwarder, nil)
	mockForwarder.EXPECT().Close().Do(func() { wg.Done() }).Return(nil)

	ff := &metricsCollectorFromFlags{
		bufferSize:         10,
		forwarderFromFlags: mockForwarderFromFlags,
	}

	mc, err := ff.Make(log)
	assert.Nil(t, err)
	mcImpl := mc.(*metricsCollector)

	assert.NotDeepEqual(t, mcImpl.forwarder, mockForwarder)

	forwarderType := reflect.TypeOf(mcImpl.forwarder).String()
	assert.Equal(t, forwarderType, "*forwarder.asyncForwarder")

	mcImpl.forwarder.Close()
	wg.Wait()
}
