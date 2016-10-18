package main

import (
	"os"

	"github.com/turbinelabs/cli"
	"github.com/turbinelabs/cli/command"
	"github.com/turbinelabs/stats/server/test"
)

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

func (r *statsRunner) Run(cmd *command.Cmd, args []string) command.CmdErr {
	if r.accessLog == "" && r.upstreamLog == "" {
		return cmd.BadInputf("one of --access.log or --upstream.log must be set")
	}

	testHarness := test.StatsServerTestHarness{
		AccessLog:              r.accessLog,
		UpstreamLog:            r.upstreamLog,
		MockWavefrontProxyPort: r.mockWavefrontProxyPort,
		MockWavefrontApiPort:   r.mockWavefrontApiPort,
		StatsServerPort:        r.statsServerPort,
	}

	if err := testHarness.Start(); err != nil {
		return cmd.Error(err.Error())
	}
	defer testHarness.Stop()

	os.Stdin.Read([]byte{0})

	return command.NoError()
}

func mkCLI() cli.CLI {
	return cli.New("0.1-dev", Cmd())
}

func main() {
	mkCLI().Main()
}
