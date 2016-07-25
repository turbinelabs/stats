package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/turbinelabs/logparser/forwarder"
	"github.com/turbinelabs/logparser/metric"
	"github.com/turbinelabs/server/http/envelope"
	httperr "github.com/turbinelabs/server/http/error"
	"github.com/turbinelabs/stats"
	"github.com/turbinelabs/test/assert"
	testio "github.com/turbinelabs/test/io"
)

const (
	testEpoch = int64(1468259800000)
)

var (
	metricSource, _ = metric.NewSource("sourcery", "")

	testEpochTime = time.Unix(testEpoch/1000, 0)
)

func makePayload(numStats int) *stats.StatsPayload {
	s := []stats.Stat{}
	for i := 0; i < numStats; i++ {
		tags := map[string]string{
			fmt.Sprintf("t%dk", i): fmt.Sprintf("t%dv", i),
		}

		stat := stats.Stat{
			Name:      fmt.Sprintf("s%d", i),
			Value:     float64(i) + 0.25,
			Timestamp: testEpoch + int64(i),
			Tags:      tags,
		}

		s = append(s, stat)
	}

	return &stats.StatsPayload{
		Source: "sourcery",
		Stats:  s,
	}
}

func makeExpectedMetricValues(numStats int) []metric.MetricValue {
	v := []metric.MetricValue{}
	for i := 0; i < numStats; i++ {
		tags := map[string]string{
			fmt.Sprintf("t%dk", i): fmt.Sprintf("t%dv", i),
		}

		m, _ := metricSource.NewMetric(fmt.Sprintf("s%d", i))

		ts := testEpochTime.Add(time.Duration(i) * time.Millisecond)

		value := metric.MetricValue{
			Metric:    m,
			Value:     float64(i) + 0.25,
			Timestamp: &ts,
			Tags:      tags,
		}

		v = append(v, value)
	}

	return v
}

func makeBytes(t *testing.T, payload *stats.StatsPayload) []byte {
	str, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json marshal error: %v", err)
	}
	return str
}

func toHttpError(t *testing.T, err error) *httperr.Error {
	switch httpErr := err.(type) {
	case httperr.Error:
		return &httpErr

	case *httperr.Error:
		return httpErr

	default:
		t.Fatal("incorrect type")
		return nil
	}
}

func TestMetricsCollectorRequestGetPayload(t *testing.T) {
	req := &http.Request{}
	payload := makePayload(2)
	req.Body = ioutil.NopCloser(bytes.NewBuffer(makeBytes(t, payload)))
	fReq := metricsCollectorRequest{req}
	reifiedPayload, err := fReq.getPayload()
	assert.Nil(t, err)
	assert.DeepEqual(t, reifiedPayload, payload)
}

func TestMetricsCollectorRequestGetPayloadNoBody(t *testing.T) {
	req := &http.Request{}
	fReq := metricsCollectorRequest{req}
	p, err := fReq.getPayload()
	assert.Nil(t, p)
	assert.NonNil(t, err)

	httpErr := toHttpError(t, err)
	assert.Equal(t, httpErr.Code, httperr.UnknownNoBodyCode)
}

func TestMetricsCollectorRequestGetPayloadBodyError(t *testing.T) {
	req := &http.Request{}
	req.Body = testio.NewFailingReader()
	fReq := metricsCollectorRequest{req}
	p, err := fReq.getPayload()
	assert.Nil(t, p)
	assert.NonNil(t, err)

	httpErr := toHttpError(t, err)
	assert.Equal(t, httpErr.Code, httperr.UnknownTransportCode)
}

func TestMetricsCollectorRequestGetPayloadUnmarshalError(t *testing.T) {
	req := &http.Request{}
	req.Body = ioutil.NopCloser(bytes.NewBuffer([]byte("this is not json")))
	fReq := metricsCollectorRequest{req}
	p, err := fReq.getPayload()
	assert.Nil(t, p)
	assert.NonNil(t, err)

	httpErr := toHttpError(t, err)
	assert.Equal(t, httpErr.Code, httperr.UnknownDecodingCode)
}

func TestAsHandler(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	c := NewMockMetricsCollector(ctrl)
	c.EXPECT().Forward(gomock.Any()).Return(1, nil)

	handler := asHandler(c)

	payload := makePayload(1)
	req := &http.Request{}
	req.Body = ioutil.NopCloser(bytes.NewBuffer(makeBytes(t, payload)))
	recorder := httptest.NewRecorder()

	handler(recorder, req)

	assert.Equal(t, recorder.Code, 200)

	expectedResult := envelope.Response{Payload: &Result{NumAccepted: 1}}
	expectedBody, err := json.Marshal(expectedResult)
	assert.Nil(t, err)

	assert.DeepEqual(t, recorder.Body.String(), string(expectedBody))
}

func TestAsHandlerForwardingError(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	httpErr := httperr.New500("herp", httperr.UnknownDecodingCode)

	c := NewMockMetricsCollector(ctrl)
	c.EXPECT().Forward(gomock.Any()).Return(1, httpErr)

	handler := asHandler(c)

	payload := makePayload(1)
	req := &http.Request{}
	req.Body = ioutil.NopCloser(bytes.NewBuffer(makeBytes(t, payload)))
	recorder := httptest.NewRecorder()

	handler(recorder, req)

	assert.Equal(t, recorder.Code, 500)

	expectedResult := envelope.Response{Error: httpErr, Payload: &Result{NumAccepted: 1}}
	expectedBody, err := json.Marshal(expectedResult)
	assert.Nil(t, err)

	assert.DeepEqual(t, recorder.Body.String(), string(expectedBody))
}

func TestAsHandlerBodyError(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	c := NewMockMetricsCollector(ctrl)

	handler := asHandler(c)

	req := &http.Request{}
	req.Body = testio.NewFailingReader()
	recorder := httptest.NewRecorder()

	handler(recorder, req)

	assert.Equal(t, recorder.Code, 500)

	response := &envelope.Response{}
	err := json.Unmarshal(recorder.Body.Bytes(), response)
	assert.Nil(t, err)
	assert.NonNil(t, response.Error)
	assert.ErrorContains(t, response.Error, "could not read request body")
}

func TestMetricsCollectorForwardInvalidSource(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockForwarder := forwarder.NewMockForwarder(ctrl)

	collector := metricsCollector{forwarder: mockForwarder}

	payload := &stats.StatsPayload{Source: "a bird in the hand"}

	sent, err := collector.Forward(payload)
	assert.Equal(t, sent, 0)
	assert.ErrorContains(t, err, "invalid metric source")
}

func TestMetricsCollectorForwardInvalidMetric(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockForwarder := forwarder.NewMockForwarder(ctrl)

	collector := metricsCollector{forwarder: mockForwarder}

	payload := makePayload(1)
	payload.Stats[0].Name = "none shall pass"

	sent, err := collector.Forward(payload)
	assert.Equal(t, sent, 0)
	assert.ErrorContains(t, err, "invalid metric")
}

type forwardTestCase struct {
	payload              *stats.StatsPayload
	expectedMetricValues []metric.MetricValue

	numSent         int
	sendErr         error
	checkForwardErr func(*testing.T, error)
}

func (tc *forwardTestCase) run(t *testing.T) {
	ctrl := gomock.NewController(assert.Tracing(t))
	defer ctrl.Finish()

	mockForwarder := forwarder.NewMockForwarder(ctrl)

	collector := metricsCollector{forwarder: mockForwarder}

	var recordedValues []metric.MetricValue
	recordMetricValues := func(v []metric.MetricValue) {
		recordedValues = append(recordedValues, v...)
	}

	mockForwarder.EXPECT().
		Send(gomock.Any()).
		Do(recordMetricValues).
		Return(tc.numSent, tc.sendErr)

	sent, err := collector.Forward(tc.payload)
	assert.Equal(t, sent, tc.numSent)
	tc.checkForwardErr(t, err)
	assert.DeepEqual(t, recordedValues, tc.expectedMetricValues)
}

func TestMetricsCollectorForward(t *testing.T) {
	tc := forwardTestCase{
		payload:              makePayload(2),
		expectedMetricValues: makeExpectedMetricValues(2),
		numSent:              2,
		checkForwardErr: func(t *testing.T, e error) {
			assert.Nil(t, e)
		},
	}
	tc.run(t)
}

func TestMetricsCollectorForwardPartialError(t *testing.T) {
	p := makePayload(3)
	p.Stats[0].Name = "first bad name"
	p.Stats[1].Name = "second bad name"

	v := makeExpectedMetricValues(3)[2:]

	tc := forwardTestCase{
		payload:              p,
		expectedMetricValues: v,
		numSent:              2,
		checkForwardErr: func(t *testing.T, err error) {
			assert.ErrorContains(t, err, "invalid metric name")
			assert.ErrorContains(t, err, "first bad name")
		},
	}
	tc.run(t)
}

func TestMetricsCollectorForwardSendError(t *testing.T) {
	err := errors.New("could not send")
	tc := forwardTestCase{
		payload:              makePayload(2),
		expectedMetricValues: makeExpectedMetricValues(2),
		numSent:              0,
		sendErr:              err,
		checkForwardErr: func(t *testing.T, e error) {
			assert.DeepEqual(t, e, err)
		},
	}
	tc.run(t)
}
