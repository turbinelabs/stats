package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/turbinelabs/cli"
	"github.com/turbinelabs/cli/command"
	clienthttp "github.com/turbinelabs/client/http"
	"github.com/turbinelabs/logparser"
	"github.com/turbinelabs/logparser/forwarder"
	"github.com/turbinelabs/logparser/metric"
	"github.com/turbinelabs/logparser/parser"
)

type MockStat struct {
	Name      string
	Value     float64
	Timestamp time.Time
	Tags      map[string]string
}

func Cmd() *command.Cmd {
	cmd := &command.Cmd{
		Name:        "stats-server-test",
		Summary:     "Turbine Labs stats server test",
		Usage:       "[OPTIONS]",
		Description: "An integration test that parses and forwards balancer stats to a mock wavefront proxy/server that supports trivial queries.",
	}

	runner := &statsRunner{}

	cmd.Flags.StringVar(
		&runner.accessLog,
		"access-log",
		"",
		"Specifies the `FILE` containing access log data. "+
			"If not specified, --upstream-log must be set.",
	)

	cmd.Flags.StringVar(
		&runner.upstreamLog,
		"upstream-log",
		"",
		"Specifies the `FILE` containing upstream log data. "+
			"If not specified, --access-log must be set.",
	)

	cmd.Flags.IntVar(
		&runner.statsServerPort,
		"api-port",
		8080,
		"Specifies the `PORT` on which the stats server will listen.",
	)

	cmd.Flags.IntVar(
		&runner.mockWavefrontProxyPort,
		"proxy-port",
		2878,
		"Specifies the `PORT` on which the mock Wavefront proxy server will listen.",
	)

	cmd.Flags.IntVar(
		&runner.mockWavefrontApiPort,
		"wavefront-api-port",
		8081,
		"Specifies the `PORT` on which the mock Wavefront API server will listen.",
	)

	cmd.Runner = runner

	return cmd
}

type statsRunner struct {
	accessLog              string
	upstreamLog            string
	statsServerPort        int
	mockWavefrontProxyPort int
	mockWavefrontApiPort   int
}

func waitForConn(name string, port int, isHttp bool) error {
	dialer := &net.Dialer{
		Timeout:   1 * time.Second,
		KeepAlive: 1 * time.Second,
	}

	if isHttp {
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
	} else {
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
	}

	return fmt.Errorf("timed out waiting for %s", name)
}

func (r *statsRunner) Run(cmd *command.Cmd, args []string) command.CmdErr {
	if r.accessLog == "" && r.upstreamLog == "" {
		return cmd.BadInputf("one of --access.log or --upstream.log must be set")
	}

	defaultSource, err := metric.NewSource(logparser.DefaultSource(), "")
	if err != nil {
		return cmd.Errorf("failed to create metric source: %s", err.Error())
	}

	httpClient := clienthttp.HeaderPreserving()
	endpoint, err := clienthttp.NewEndpoint(clienthttp.HTTP, "127.0.0.1", r.statsServerPort)
	if err != nil {
		return cmd.Errorf("failed to create endpoint: %s", err.Error())
	}

	statsChannel, err := startMockWavefrontProxy(r.mockWavefrontProxyPort)
	if err != nil {
		return cmd.Errorf("failed to start mock wavefront proxy: %s", err.Error())
	}
	err = startMockWavefrontApi(r.mockWavefrontApiPort, statsChannel)
	if err != nil {
		return cmd.Errorf("failed to start mock wavefront api: %s", err.Error())
	}

	proc, err := startStatsApi(
		r.statsServerPort,
		r.mockWavefrontProxyPort,
		r.mockWavefrontApiPort,
	)
	if err != nil {
		return cmd.Errorf("failed to start stats-server: %s", err.Error())
	}
	defer proc.Quit()

	if err := waitForConn("mock-wavefront-proxy", r.mockWavefrontProxyPort, false); err != nil {
		return cmd.Errorf("failed to detect mock wavefront proxy: %s", err.Error())
	}
	if err := waitForConn("mock-wavefront-api", r.mockWavefrontApiPort, true); err != nil {
		return cmd.Errorf("failed to detect mock wavefront api: %s", err.Error())
	}
	if err := waitForConn("stats-server", r.statsServerPort, true); err != nil {
		return cmd.Errorf("failed to detect stats server: %s", err.Error())
	}

	var accessLogParser logparser.LogParser
	if r.accessLog != "" {
		parser, err := parser.NewPositionalDelimiter(
			&defaultSource,
			parser.TbnAccessFormat,
			parser.DefaultPositionalDelimiterSet,
			parser.NginxTimeIso8601Parser,
		)
		if err != nil {
			return cmd.Errorf("failed to create access log parser: %s", err.Error())
		}

		forwarder, err := forwarder.NewAPIForwarder(
			httpClient,
			endpoint,
			"IMOK",
			"test-zone-name",
		)
		if err != nil {
			return cmd.Errorf("failed to create access log forwarder: %s", err.Error())
		}

		accessLogParser = logparser.New(parser, forwarder)
		go func() {
			if err := accessLogParser.Tail(r.accessLog); err != nil {
				fmt.Printf(
					"tailing access log '%s' failed: %s",
					r.accessLog,
					err.Error(),
				)
			}
		}()

		defer accessLogParser.Close()
	}

	var upstreamLogParser logparser.LogParser
	if r.upstreamLog != "" {
		parser, err := parser.NewPositionalDelimiter(
			&defaultSource,
			parser.TbnUpstreamFormat,
			parser.DefaultPositionalDelimiterSet,
			parser.NginxTimeIso8601Parser,
		)
		if err != nil {
			return cmd.Errorf("failed to create upstream log parser: %s", err.Error())
		}

		forwarder, err := forwarder.NewAPIForwarderForUpstreams(
			httpClient,
			endpoint,
			"IMOK",
			"test-zone-name",
		)
		if err != nil {
			return cmd.Errorf(
				"failed to create upstream log forwarder: %s",
				err.Error(),
			)
		}

		upstreamLogParser = logparser.New(parser, forwarder)
		go func() {
			if err := upstreamLogParser.Tail(r.upstreamLog); err != nil {
				fmt.Printf(
					"tailing upstream log '%s' failed: %s",
					r.upstreamLog,
					err.Error(),
				)
			}
		}()

		defer upstreamLogParser.Close()
	}

	os.Stdin.Read([]byte{0})

	return command.NoError()
}

func mkCLI() cli.CLI {
	return cli.New("0.1-dev", Cmd())
}

func main() {
	mkCLI().Main()
}
