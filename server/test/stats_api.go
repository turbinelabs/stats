package test

import (
	"fmt"

	"github.com/turbinelabs/nonstdlib/proc"
)

func StartStatsApi(port, proxyPort, wavefrontApiPort int) (proc.ManagedProc, error) {
	p := proc.NewDefaultManagedProc(
		"stats-server",
		[]string{
			"--dev=noauth",
			"--listener.https.disabled=true",
			"--listener.ip=0.0.0.0",
			fmt.Sprintf("--listener.port=%d", port),
			"--listener.stats.disabled=true",
			"--stats.forwarder=wavefront",
			fmt.Sprintf("--stats.wavefront.host=127.0.0.1:%d", proxyPort),
			fmt.Sprintf("--wavefront-api.url=http://127.0.0.1:%d", wavefrontApiPort),
			"--wavefront-api.token=BOB_IS_UR_UNKLE",
		},
		func(err error) {
			fmt.Printf("stats-server exited with error: %s\n", err.Error())
		},
	)

	return p, p.Start()
}
