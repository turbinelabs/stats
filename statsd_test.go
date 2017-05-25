package stats

import (
	"fmt"
	"net"
	"testing"
	"time"

	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
	"github.com/turbinelabs/test/assert"
)

type testUDPListener struct {
	Msgs chan string

	conn *net.UDPConn
}

func (l *testUDPListener) Addr(t *testing.T) string {
	if l.conn == nil {
		t.Fatal("no connection")
	}

	return l.conn.LocalAddr().String()
}

func (l *testUDPListener) Close() error {
	if l.conn == nil {
		return nil
	}

	conn := l.conn
	l.conn = nil
	return conn.Close()
}

func mkListener(t *testing.T) *testUDPListener {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
	if err != nil {
		t.Fatalf("could not open connection: %s", err.Error())
	}

	msgs := make(chan string, 10)

	go func() {
		buffer := make([]byte, 8192)
		for {
			n, err := conn.Read(buffer)
			if n > 0 {
				msgs <- string(buffer[0:n])
			} else if err != nil {
				break
			}
		}
	}()

	return &testUDPListener{Msgs: msgs, conn: conn}
}

func TestStatsdBackend(t *testing.T) {
	l := mkListener(t)
	defer l.Close()

	addr := l.Addr(t)
	_, port, err := tbnstrings.SplitHostPort(addr)
	assert.Nil(t, err)

	statsdFromFlags := &statsdFromFlags{
		host:          "127.0.0.1",
		port:          port,
		flushInterval: 10 * time.Millisecond,
	}

	stats, err := statsdFromFlags.Make()
	assert.Nil(t, err)
	defer stats.Close()

	scope := stats.Scope("prefix")

	scope.Count("count", 2.0, NewKVTag("nopity", "nope"))
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("prefix.count:%f|c\n", 2.0))

	scope.Gauge("gauge", 3.0)
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("prefix.gauge:%f|g\n", 3.0))
}
