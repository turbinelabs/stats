package main

import (
	"github.com/turbinelabs/cli"
	"github.com/turbinelabs/cli/command"
	"github.com/turbinelabs/stats/server"
)

func Cmd() *command.Cmd {
	cmd := &command.Cmd{
		Name:        "stats-server",
		Summary:     "Turbine Labs stats server",
		Usage:       "[OPTIONS]",
		Description: "Handle requests forwarding statistics from customer sites.",
	}

	cmd.Runner = &statsRunner{
		server.NewFromFlags(&cmd.Flags),
	}

	return cmd
}

type statsRunner struct {
	flags server.FromFlags
}

func (r *statsRunner) Run(cmd *command.Cmd, args []string) command.CmdErr {
	if err := r.flags.Validate(); err != nil {
		return cmd.BadInput(err)
	}

	server, err := r.flags.Make()
	if err != nil {
		return cmd.Error(err)
	}

	err = server.Start()
	if err != nil {
		return cmd.Error(err)
	}

	return command.NoError()
}

func mkCLI() cli.CLI {
	return cli.New("0.1-dev", Cmd())
}

func main() {
	mkCLI().Main()
}
