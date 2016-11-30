package client

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	statsapi "github.com/turbinelabs/api/service/stats"
	"github.com/turbinelabs/nonstdlib/executor"
	tbntime "github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/test/assert"
	"github.com/turbinelabs/test/log"
)

func payloadOfSize(s int) *statsapi.Payload {
	switch s {
	case 0:
		return &statsapi.Payload{Source: sourceString1}

	case 1:
		return payload

	default:
		a := make([]statsapi.Stat, s)
		for i := 0; i < s; i++ {
			a[i] = payload.Stats[0]
		}
		return &statsapi.Payload{Source: sourceString1, Stats: a}
	}
}

type batcherTest struct {
	expectedPayloadSizes  []int // the bathces: N payloads of given sizes
	numForwards           int   // number of payloads enqueued
	forwardedSize         int   // size of each forwarded payload
	closeAfterLastPayload bool

	maxDelay time.Duration // batcher timer setting
	maxSize  int           // batcher max payload size setting

	timerBehavior func(*tbntime.MockTimer)
}

func (bt batcherTest) run(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	cbfChan := make(chan executor.CallbackFunc, 2)

	mockUnderlyingStatsClient := NewMockinternalStatsClient(ctrl)

	for _, payloadSize := range bt.expectedPayloadSizes {
		expectedPayload := payloadOfSize(payloadSize)
		mockUnderlyingStatsClient.EXPECT().
			IssueRequest(expectedPayload, gomock.Any()).
			Do(func(_ *statsapi.Payload, cb executor.CallbackFunc) { cbfChan <- cb }).
			Return(nil)
	}

	batcher := &payloadBatcher{
		client: &httpBatchingStatsV1{
			internalStatsClient: mockUnderlyingStatsClient,
			maxDelay:            bt.maxDelay,
			maxSize:             bt.maxSize,
		},
		source: sourceString1,
		ch:     make(chan *statsapi.Payload, 2*bt.maxSize),
	}

	mockTimer := tbntime.NewMockTimer(ctrl)

	bt.timerBehavior(mockTimer)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer wg.Wait()

	go func() {
		defer wg.Done()
		batcher.run(mockTimer)
	}()
	if !bt.closeAfterLastPayload {
		defer close(batcher.ch)
	}

	for i := 0; i < bt.numForwards; i++ {
		batcher.ch <- payloadOfSize(bt.forwardedSize)
	}
	if bt.closeAfterLastPayload {
		close(batcher.ch)
	}

	for i := 0; i < len(bt.expectedPayloadSizes); i++ {
		<-cbfChan
	}
}

func TestNewBatchingStatsClient(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	exec := executor.NewMockExecutor(ctrl)
	client, err := NewBatchingStatsClient(
		time.Second,
		100,
		endpoint,
		testApiKey,
		exec,
		log.NewNoopLogger(),
	)
	assert.NonNil(t, client)
	assert.Nil(t, err)

	clientImpl, ok := client.(*httpBatchingStatsV1)
	assert.True(t, ok)

	assert.NonNil(t, clientImpl.internalStatsClient)
	underlyingImpl, ok := clientImpl.internalStatsClient.(*httpStatsV1)
	assert.True(t, ok)
	assert.DeepEqual(t, underlyingImpl.dest, endpoint)
	assert.SameInstance(t, underlyingImpl.exec, exec)

	assert.Equal(t, clientImpl.maxDelay, time.Second)
	assert.Equal(t, clientImpl.maxSize, 100)
	assert.NonNil(t, clientImpl.batchers)
	assert.Equal(t, len(clientImpl.batchers), 0)
	assert.NonNil(t, clientImpl.mutex)
}

func TestNewBatchingStatsClientValidation(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	exec := executor.NewMockExecutor(ctrl)
	log := log.NewNoopLogger()

	client, err := NewBatchingStatsClient(
		999*time.Millisecond,
		1,
		endpoint,
		testApiKey,
		exec,
		log,
	)
	assert.Nil(t, client)
	assert.ErrorContains(t, err, "max delay must be at least 1 second")

	client, err = NewBatchingStatsClient(
		time.Second,
		0,
		endpoint,
		testApiKey,
		exec,
		log,
	)
	assert.Nil(t, client)
	assert.ErrorContains(t, err, "max size must be at least 1")
}

func TestHttpBatchingStatsV1NewBatcher(t *testing.T) {
	client := &httpBatchingStatsV1{
		batchers: map[string]*payloadBatcher{},
		mutex:    &sync.RWMutex{},
	}

	batcher := client.newBatcher(sourceString1)
	defer close(batcher.ch)

	assert.NonNil(t, batcher)
	assert.SameInstance(t, batcher.client, client)
	assert.Equal(t, batcher.source, sourceString1)
	assert.NonNil(t, batcher.ch)

	batcher2 := client.newBatcher(sourceString1)
	assert.SameInstance(t, batcher2, batcher)
}

func TestHttpBatchingStatsV1GetBatcher(t *testing.T) {
	client := &httpBatchingStatsV1{
		batchers: map[string]*payloadBatcher{},
		mutex:    &sync.RWMutex{},
	}

	assert.Nil(t, client.getBatcher(sourceString1))

	batcher := client.newBatcher(sourceString1)
	defer close(batcher.ch)

	assert.SameInstance(t, client.getBatcher(sourceString1), batcher)
}

func TestHttpBatchingStatsV1Forward(t *testing.T) {
	client := &httpBatchingStatsV1{
		batchers: map[string]*payloadBatcher{},
		mutex:    &sync.RWMutex{},
	}

	expectedPayload := payloadOfSize(3)

	result, err := client.Forward(expectedPayload)
	assert.NonNil(t, result)
	assert.Nil(t, err)
	assert.Equal(t, result.NumAccepted, 3)

	batcher, ok := client.batchers[expectedPayload.Source]
	assert.True(t, ok)
	assert.NonNil(t, batcher)
	defer close(batcher.ch)

	select {
	case payload := <-batcher.ch:
		assert.SameInstance(t, payload, expectedPayload)

	default:
		assert.Failed(t, "payload not enqueued in batcher's channel")
	}
}

func TestHttpBatchingStatsV1Close(t *testing.T) {
	client := &httpBatchingStatsV1{
		batchers: map[string]*payloadBatcher{},
		mutex:    &sync.RWMutex{},
	}

	client.newBatcher("this-source")
	client.newBatcher("that-source")
	assert.Equal(t, len(client.batchers), 2)

	ch1 := client.batchers["this-source"].ch
	ch2 := client.batchers["that-source"].ch

	assert.Nil(t, client.Close())
	assert.Equal(t, len(client.batchers), 0)

	select {
	case _, ok := <-ch1:
		assert.False(t, ok)
	default:
		assert.Failed(t, "expected closed channel ch1, saw empty channel")
	}

	select {
	case _, ok := <-ch2:
		assert.False(t, ok)
	default:
		assert.Failed(t, "expected closed channel ch2, saw empty channel")
	}
}

func TestPayloadBatcherRunSendsBatchBySize(t *testing.T) {
	batcherTest{
		expectedPayloadSizes: []int{5},
		numForwards:          5,
		forwardedSize:        1,
		maxDelay:             time.Second,
		maxSize:              5,
		timerBehavior: func(mockTimer *tbntime.MockTimer) {
			emptyTimeChan := make(chan time.Time, 1)

			gomock.InOrder(
				mockTimer.EXPECT().Stop().Return(true),
				mockTimer.EXPECT().Reset(1*time.Second).Return(false),
				mockTimer.EXPECT().C().Times(5).Return(emptyTimeChan),
				mockTimer.EXPECT().Stop().Return(true),
				mockTimer.EXPECT().C().Return(emptyTimeChan),
				mockTimer.EXPECT().Stop().Return(false),
			)
		},
	}.run(t)
}

func TestPayloadBatcherRunSendsBatchBySizeOnFirstCall(t *testing.T) {
	batcherTest{
		expectedPayloadSizes: []int{5},
		numForwards:          1,
		forwardedSize:        5,
		maxDelay:             time.Second,
		maxSize:              3,
		timerBehavior: func(mockTimer *tbntime.MockTimer) {
			emptyTimeChan := make(chan time.Time, 1)

			gomock.InOrder(
				mockTimer.EXPECT().Stop().Return(true),
				mockTimer.EXPECT().Reset(1*time.Second).Return(false),
				mockTimer.EXPECT().C().Times(1).Return(emptyTimeChan),
				mockTimer.EXPECT().Stop().Return(true),
				mockTimer.EXPECT().C().Return(emptyTimeChan),
				mockTimer.EXPECT().Stop().Return(false),
			)
		},
	}.run(t)
}

func TestPayloadBatcherRunSendsBatchByDelay(t *testing.T) {
	batcherTest{
		expectedPayloadSizes: []int{5},
		numForwards:          5,
		forwardedSize:        1,
		maxDelay:             time.Second,
		maxSize:              50,
		timerBehavior: func(mockTimer *tbntime.MockTimer) {
			emptyTimeChan := make(chan time.Time, 1)
			deadlineTimeChan := make(chan time.Time, 1)
			deadlineTimeChan <- time.Now()

			gomock.InOrder(
				mockTimer.EXPECT().Stop().Return(true),
				mockTimer.EXPECT().Reset(1*time.Second).Return(false),
				mockTimer.EXPECT().C().Times(5).Return(emptyTimeChan),
				mockTimer.EXPECT().C().Return(deadlineTimeChan),
				mockTimer.EXPECT().C().Return(emptyTimeChan),
				mockTimer.EXPECT().Stop().Return(false),
			)
		},
	}.run(t)
}

func TestPayloadBatcherRunResetsTimer(t *testing.T) {
	batcherTest{
		expectedPayloadSizes: []int{5, 1},
		numForwards:          6,
		forwardedSize:        1,
		maxDelay:             time.Second,
		maxSize:              5,
		timerBehavior: func(mockTimer *tbntime.MockTimer) {
			emptyTimeChan := make(chan time.Time, 1)
			deadlineTimeChan := make(chan time.Time, 1)
			deadlineTimeChan <- time.Now()

			gomock.InOrder(
				mockTimer.EXPECT().Stop().Return(true),
				mockTimer.EXPECT().Reset(1*time.Second).Return(false),
				mockTimer.EXPECT().C().Times(5).Return(emptyTimeChan),
				mockTimer.EXPECT().Stop().Return(true),
				mockTimer.EXPECT().C().Times(1).Return(emptyTimeChan),
				mockTimer.EXPECT().Reset(1*time.Second).Return(false),
				mockTimer.EXPECT().C().Return(deadlineTimeChan),
				mockTimer.EXPECT().C().Return(emptyTimeChan),
				mockTimer.EXPECT().Stop().Return(false),
			)
		},
	}.run(t)
}

func TestPayloadBatcherRunSendsOnClose(t *testing.T) {
	batcherTest{
		expectedPayloadSizes:  []int{3},
		numForwards:           1,
		forwardedSize:         3,
		closeAfterLastPayload: true,
		maxDelay:              time.Second,
		maxSize:               5,
		timerBehavior: func(mockTimer *tbntime.MockTimer) {
			emptyTimeChan := make(chan time.Time, 1)

			gomock.InOrder(
				mockTimer.EXPECT().Stop().Return(true),
				mockTimer.EXPECT().Reset(1*time.Second).Return(false),
				mockTimer.EXPECT().C().Times(2).Return(emptyTimeChan),
				mockTimer.EXPECT().Stop().Return(false),
			)
		},
	}.run(t)
}
