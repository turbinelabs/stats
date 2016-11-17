package client

import (
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	tbnhttp "github.com/turbinelabs/client/http"
	"github.com/turbinelabs/nonstdlib/executor"
	tbntime "github.com/turbinelabs/nonstdlib/time"
	"github.com/turbinelabs/stats"
)

type httpBatchingStatsV1 struct {
	internalStatsClient

	maxDelay time.Duration
	maxSize  int

	batchers map[string]*payloadBatcher
	mutex    *sync.RWMutex

	logger *log.Logger
}

// NewBatchingStatsClient returns a non-blocking implementation of
// StatsClient. Each invocation of Forward accepts a single
// Payload. The client will return immediately, reporting that all
// stats were successfully sent. Internally, the stats are buffered
// until the buffer contains at least maxSize stats or maxDelay time
// has elapsed since the oldest stats in the buffer were added. At
// that point the buffered stats are forwarded. Failures are logged,
// but not reported to the caller. Separate buffers and deadlines are
// maintained for each unique source.
func NewBatchingStatsClient(
	maxDelay time.Duration,
	maxSize int,
	dest tbnhttp.Endpoint,
	apiKey string,
	client *http.Client,
	exec executor.Executor,
	logger *log.Logger,
) (StatsClient, error) {
	if maxDelay < time.Second {
		return nil, errors.New("max delay must be at least 1 second")
	}

	if maxSize < 1 {
		return nil, errors.New("max size must be at least 1")
	}

	underlyingStatsClient, err := newInternalStatsClient(dest, apiKey, client, exec)
	if err != nil {
		return nil, err
	}

	return &httpBatchingStatsV1{
		internalStatsClient: underlyingStatsClient,
		maxDelay:            maxDelay,
		maxSize:             maxSize,
		batchers:            map[string]*payloadBatcher{},
		mutex:               &sync.RWMutex{},
		logger:              logger,
	}, nil
}

func (hs *httpBatchingStatsV1) getBatcher(source string) *payloadBatcher {
	hs.mutex.RLock()
	defer hs.mutex.RUnlock()

	return hs.batchers[source]
}

func (hs *httpBatchingStatsV1) newBatcher(source string) *payloadBatcher {
	hs.mutex.Lock()
	defer hs.mutex.Unlock()

	batcher, ok := hs.batchers[source]
	if !ok {
		batcher = &payloadBatcher{
			client: hs,
			source: source,
			ch:     make(chan *stats.StatsPayload, 10),
		}

		hs.batchers[source] = batcher
		batcher.start()
	}

	return batcher
}

func (hs *httpBatchingStatsV1) Forward(payload *stats.StatsPayload) (*stats.Result, error) {
	batcher := hs.getBatcher(payload.Source)
	if batcher == nil {
		batcher = hs.newBatcher(payload.Source)
	}

	batcher.ch <- payload

	return &stats.Result{NumAccepted: len(payload.Stats)}, nil
}

func (hs *httpBatchingStatsV1) Close() error {
	hs.mutex.Lock()
	defer hs.mutex.Unlock()

	for _, batcher := range hs.batchers {
		close(batcher.ch)
	}
	hs.batchers = map[string]*payloadBatcher{}
	return nil
}

func (hs *httpBatchingStatsV1) Stats(source string, scope ...string) stats.Stats {
	return newStats(hs, source, scope...)
}

type payloadBatcher struct {
	client *httpBatchingStatsV1
	source string
	ch     chan *stats.StatsPayload
}

func (b *payloadBatcher) start() {
	go b.run(tbntime.NewTimer(0))
}

func (b *payloadBatcher) run(timer tbntime.Timer) {
	buffer := make([]stats.Stat, 0, b.client.maxSize)

	if !timer.Stop() {
		<-timer.C()
	}
	timer.Reset(b.client.maxDelay)
	timerIsLive := true

	for {
		select {
		case <-timer.C():
			if len(buffer) > 0 {
				b.forward(buffer)
				buffer = buffer[0:0]
			}
			timerIsLive = false

		case payload, ok := <-b.ch:
			if !ok {
				if len(buffer) > 0 {
					b.forward(buffer)
					buffer = buffer[0:0]
				}
				timer.Stop()
				return
			}
			buffer = append(buffer, payload.Stats...)
			if len(buffer) >= b.client.maxSize {
				timer.Stop()
				timerIsLive = false
				b.forward(buffer)
				buffer = buffer[0:0]
			} else if !timerIsLive {
				timer.Reset(b.client.maxDelay)
			}
		}
	}
}

func (b *payloadBatcher) forward(s []stats.Stat) {
	payload := &stats.StatsPayload{
		Source: b.source,
		Stats:  s,
	}

	err := b.client.IssueRequest(
		payload,
		func(try executor.Try) {
			if try.IsError() {
				b.client.logger.Printf(
					"Failed to forward payload: %+v: %s",
					payload,
					try.Error().Error(),
				)
			}
		},
	)
	if err != nil {
		b.client.logger.Printf(
			"Failed to enqueue request: %+v: %s",
			payload,
			err.Error(),
		)
	}
}
