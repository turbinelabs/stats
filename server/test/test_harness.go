package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"text/template"
	"time"

	clienthttp "github.com/turbinelabs/client/http"
	"github.com/turbinelabs/logparser"
	"github.com/turbinelabs/logparser/forwarder"
	"github.com/turbinelabs/logparser/metric"
	"github.com/turbinelabs/logparser/parser"
	"github.com/turbinelabs/nonstdlib/executor"
	"github.com/turbinelabs/nonstdlib/proc"
	"github.com/turbinelabs/server/header"
	"github.com/turbinelabs/server/http/envelope"
	"github.com/turbinelabs/stats/server/handler"
	"github.com/turbinelabs/test/tempfile"
)

const (
	TestOrgKey   = handler.NoAuthOrgKey
	TestZoneName = "test-zone-name"

	logTimeFormat = `2006-01-02T15:04:05-07:00`
)

type StatsServerTestHarness struct {
	AccessLog   string
	UpstreamLog string

	StatsServerPort        int
	MockWavefrontProxyPort int
	MockWavefrontApiPort   int

	LogStartTime time.Time

	onClose            []func() error
	statsApiProc       proc.ManagedProc
	mockWavefrontProxy *MockWavefrontProxy
	mockWavefrontApi   *MockWavefrontApi
}

func (s *StatsServerTestHarness) StatsApiProc() proc.ManagedProc {
	return s.statsApiProc
}

func (s *StatsServerTestHarness) MockWavefrontProxy() *MockWavefrontProxy {
	return s.mockWavefrontProxy
}

func (s *StatsServerTestHarness) MockWavefrontApi() *MockWavefrontApi {
	return s.mockWavefrontApi
}

func NewStatsServerTestHarness() *StatsServerTestHarness {
	return &StatsServerTestHarness{
		StatsServerPort:        8080,
		MockWavefrontProxyPort: 2878,
		MockWavefrontApiPort:   8081,
		onClose:                make([]func() error, 0, 5),
		LogStartTime:           time.Now().Truncate(time.Hour).UTC(),
	}
}

func (s *StatsServerTestHarness) Start() error {
	log := log.New(os.Stderr, "", log.LstdFlags)

	invokeStop := true
	defer func() {
		if invokeStop {
			s.Stop()
		}
	}()

	defaultSource, err := metric.NewSource(logparser.DefaultSource(), "")
	if err != nil {
		return fmt.Errorf("failed to create metric source: %s", err.Error())
	}

	httpClient := clienthttp.HeaderPreserving()
	endpoint, err := clienthttp.NewEndpoint(clienthttp.HTTP, "127.0.0.1", s.StatsServerPort)
	if err != nil {
		return fmt.Errorf("failed to create endpoint: %s", err.Error())
	}

	s.mockWavefrontProxy, err = StartMockWavefrontProxy(s.MockWavefrontProxyPort)
	if err != nil {
		return fmt.Errorf("failed to start mock wavefront proxy: %s", err.Error())
	}
	s.onClose = append(s.onClose, s.mockWavefrontProxy.Close)

	s.mockWavefrontApi, err = StartMockWavefrontApi(
		s.MockWavefrontApiPort,
		s.mockWavefrontProxy.Stats,
	)
	if err != nil {
		return fmt.Errorf("failed to start mock wavefront api: %s", err.Error())
	}
	s.onClose = append(s.onClose, s.mockWavefrontApi.Close)

	s.statsApiProc, err = StartStatsApi(
		s.StatsServerPort,
		s.MockWavefrontProxyPort,
		s.MockWavefrontApiPort,
	)
	if err != nil {
		return fmt.Errorf("failed to start stats-server: %s", err.Error())
	}
	s.onClose = append(s.onClose, func() error {
		if err := s.statsApiProc.Quit(); err != nil {
			return err
		}
		return s.statsApiProc.Wait()
	})

	if err := waitForTcp("mock-wavefront-proxy", s.MockWavefrontProxyPort); err != nil {
		return fmt.Errorf("failed to detect mock wavefront proxy: %s", err.Error())
	}
	if err := waitForHttp("mock-wavefront-api", s.MockWavefrontApiPort); err != nil {
		return fmt.Errorf("failed to detect mock wavefront api: %s", err.Error())
	}
	if err := waitForHttp("stats-server", s.StatsServerPort); err != nil {
		return fmt.Errorf("failed to detect stats server: %s", err.Error())
	}

	exec := executor.NewRetryingExecutor(
		executor.WithRetryDelayFunc(
			executor.NewExponentialDelayFunc(100*time.Millisecond, 30*time.Second),
		),
		executor.WithMaxAttempts(8),
		executor.WithMaxQueueDepth(runtime.NumCPU()*20),
		executor.WithParallelism(runtime.NumCPU()*2),
	)

	var accessLogParser logparser.LogParser
	if s.AccessLog != "" {
		parser, err := parser.NewPositionalDelimiter(
			&defaultSource,
			parser.TbnAccessFormat,
			parser.DefaultPositionalDelimiterSet,
			parser.NginxTimeIso8601Parser,
		)
		if err != nil {
			return fmt.Errorf("failed to create access log parser: %s", err.Error())
		}

		forwarder, err := forwarder.NewAPIForwarder(
			log,
			httpClient,
			endpoint,
			"IMOK",
			TestZoneName,
			exec,
		)
		if err != nil {
			return fmt.Errorf("failed to create access log forwarder: %s", err.Error())
		}

		accessLogParser = logparser.New(parser, forwarder)
		go func() {
			if err := accessLogParser.Tail(s.AccessLog); err != nil {
				fmt.Printf(
					"tailing access log '%s' failed: %s",
					s.AccessLog,
					err.Error(),
				)
			}
		}()

		s.onClose = append(s.onClose, accessLogParser.Close)
	}

	var upstreamLogParser logparser.LogParser
	if s.UpstreamLog != "" {
		parser, err := parser.NewPositionalDelimiter(
			&defaultSource,
			parser.TbnUpstreamFormat,
			parser.DefaultPositionalDelimiterSet,
			parser.NginxTimeIso8601Parser,
		)
		if err != nil {
			return fmt.Errorf("failed to create upstream log parser: %s", err.Error())
		}

		forwarder, err := forwarder.NewAPIForwarderForUpstreams(
			log,
			httpClient,
			endpoint,
			"IMOK",
			TestZoneName,
			exec,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to create upstream log forwarder: %s",
				err.Error(),
			)
		}

		upstreamLogParser = logparser.New(parser, forwarder)
		go func() {
			if err := upstreamLogParser.Tail(s.UpstreamLog); err != nil {
				fmt.Printf(
					"tailing upstream log '%s' failed: %s",
					s.UpstreamLog,
					err.Error(),
				)
			}
		}()

		s.onClose = append(s.onClose, upstreamLogParser.Close)
	}

	invokeStop = false
	return nil
}

func waitForHttp(name string, port int) error {
	dialer := &net.Dialer{
		Timeout:   1 * time.Second,
		KeepAlive: 1 * time.Second,
	}

	client := clienthttp.HeaderPreserving()
	client.Transport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		MaxIdleConns:          10,
		IdleConnTimeout:       1 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	start := time.Now()
	for time.Since(start) < 10*time.Second {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/not-found", port))
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		if err != nil || resp.StatusCode > 404 {
			fmt.Println("waiting for", name)
			time.Sleep(1 * time.Second)
		} else {
			return nil
		}
	}

	return fmt.Errorf("timed out waiting for %s", name)
}

func waitForTcp(name string, port int) error {
	dialer := &net.Dialer{
		Timeout:   1 * time.Second,
		KeepAlive: 1 * time.Second,
	}

	start := time.Now()
	for time.Since(start) < 10*time.Second {
		conn, err := dialer.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			fmt.Println("waiting for", name)
			time.Sleep(1 * time.Second)
		} else {
			conn.Close()
			return nil
		}
	}

	return fmt.Errorf("timed out waiting for %s", name)
}

func (s *StatsServerTestHarness) Stop() {
	fmt.Println("StatsServerTestHarness stopping")
	for i := len(s.onClose) - 1; i >= 0; i-- {
		s.onClose[i]()
	}
}

func (s *StatsServerTestHarness) WriteAccessLogFile(
	t *testing.T,
	lineTemplate string,
	entries []interface{},
) {
	log := s.writeLogFile(t, "access_log", lineTemplate, entries)
	s.AccessLog = log
}

func (s *StatsServerTestHarness) WriteUpstreamLogFile(
	t *testing.T,
	lineTemplate string,
	entries []interface{},
) {
	log := s.writeLogFile(t, "upstream_log", lineTemplate, entries)
	s.UpstreamLog = log
}

func (s *StatsServerTestHarness) writeLogFile(
	t *testing.T,
	name, lineTemplate string,
	entries []interface{},
) string {
	if !strings.HasSuffix(lineTemplate, "\n") {
		lineTemplate += "\n"
	}

	fm := template.FuncMap{
		"Timestamp": func(offsetSeconds int) string {
			t := s.LogStartTime.Add(time.Duration(offsetSeconds) * time.Second)
			return t.Format(logTimeFormat)
		},
	}

	tmpl, err := template.New(name).Funcs(fm).Parse(lineTemplate)
	if err != nil {
		t.Fatalf("could not parse %s template: %+v", name, err)
		return ""
	}

	b := bytes.NewBuffer(make([]byte, 0, 4096))
	for i, entry := range entries {
		if err := tmpl.Execute(b, entry); err != nil {
			t.Fatalf("could not generate %s line %d: %+v", name, i+1, err)
			return ""
		}
	}

	file, cleanup := tempfile.Write(t, b.String(), name)

	s.onClose = append(s.onClose, func() error { cleanup(); return nil })

	return file
}

func (s *StatsServerTestHarness) Query(q *handler.StatsQuery) (*handler.StatsQueryResult, error) {
	queryJson, err := json.Marshal(q)
	if err != nil {
		fmt.Println("marshal error", err)
		return nil, err
	}

	queryUrl := fmt.Sprintf(
		"http://localhost:%d/v1.0/stats/query?query=%s",
		s.StatsServerPort,
		url.QueryEscape(string(queryJson)),
	)

	request, err := http.NewRequest("GET", queryUrl, nil)
	if err != nil {
		fmt.Println("request error", err)
		return nil, err
	}
	request.Header.Add(header.APIKey, "IMOK")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		fmt.Println("request execution error", err)
		return nil, err
	}
	defer response.Body.Close()

	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("io error", err)
		return nil, err
	}

	envelope := &envelope.Response{Payload: &handler.StatsQueryResult{}}
	if err := json.Unmarshal(content, envelope); err != nil {
		fmt.Println("unmarshal error", err)
		return nil, err
	}

	if envelope.Error != nil {
		return nil, envelope.Error
	}

	result, ok := envelope.Payload.(*handler.StatsQueryResult)
	if !ok {
		return nil, fmt.Errorf("got unexpected payload type %T", envelope.Payload)
	}

	return result, nil
}
