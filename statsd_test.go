package stats

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	tbnstrings "github.com/turbinelabs/nonstdlib/strings"
	"github.com/turbinelabs/test/assert"
	testio "github.com/turbinelabs/test/io"
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
		lsff:          &latchingSenderFromFlags{},
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

func TestStatsdBackendWithScope(t *testing.T) {
	l := mkListener(t)
	defer l.Close()

	addr := l.Addr(t)
	_, port, err := tbnstrings.SplitHostPort(addr)
	assert.Nil(t, err)

	statsdFromFlags := &statsdFromFlags{
		host:          "127.0.0.1",
		port:          port,
		flushInterval: 10 * time.Millisecond,
		lsff:          &latchingSenderFromFlags{},
		scope:         "x",
	}

	stats, err := statsdFromFlags.Make()
	assert.Nil(t, err)
	defer stats.Close()

	scope := stats.Scope("prefix")

	scope.Count("count", 2.0, NewKVTag("nopity", "nope"))
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("x.prefix.count:%f|c\n", 2.0))

	scope.Gauge("gauge", 3.0)
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("x.prefix.gauge:%f|g\n", 3.0))
}

func TestStatsdStdoutHook(t *testing.T) {
	l := mkListener(t)
	defer l.Close()

	addr := l.Addr(t)
	_, port, err := tbnstrings.SplitHostPort(addr)
	assert.Nil(t, err)

	var savedStdoutWriter io.Writer
	msgs := make(chan string, 10)
	writer := testio.NewChannelWriter(msgs)

	savedStdoutWriter, stdoutWriter = stdoutWriter, writer
	defer func() {
		stdoutWriter = savedStdoutWriter
	}()

	statsdFromFlags := &statsdFromFlags{
		host:          "127.0.0.1",
		port:          port,
		flushInterval: 10 * time.Millisecond,
		debug:         true,
		lsff:          &latchingSenderFromFlags{},
	}

	stats, err := statsdFromFlags.Make()
	assert.Nil(t, err)
	defer stats.Close()

	scope := stats.Scope("prefix")

	scope.Count("count", 2.0, NewKVTag("nopity", "nope"))
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("prefix.count:%f|c\n", 2.0))
	assert.Equal(t, <-msgs, fmt.Sprintf("prefix.count:%f|c\n", 2.0))

	scope.Gauge("gauge", 3.0)
	assert.Equal(t, <-l.Msgs, fmt.Sprintf("prefix.gauge:%f|g\n", 3.0))
	assert.Equal(t, <-msgs, fmt.Sprintf("prefix.gauge:%f|g\n", 3.0))
}

func TestDebugWriter(t *testing.T) {
	main := bytes.NewBuffer(make([]byte, 0, 1024))
	debug := bytes.NewBuffer(make([]byte, 0, 1024))
	failing := testio.NewFailingWriter()

	dw := &debugWriter{main, debug}
	n, err := dw.Write([]byte("both"))
	assert.Equal(t, n, 4)
	assert.Nil(t, err)
	assert.Equal(t, main.String(), "both")
	assert.Equal(t, debug.String(), "both")

	main.Reset()
	debug.Reset()

	dw = &debugWriter{main, failing}
	n, err = dw.Write([]byte("both"))
	assert.Equal(t, n, 4)
	assert.Nil(t, err)
	assert.Equal(t, main.String(), "both")

	main.Reset()
	debug.Reset()

	dw = &debugWriter{failing, debug}
	n, err = dw.Write([]byte("both"))
	assert.Equal(t, n, 0)
	assert.NonNil(t, err)
	assert.Equal(t, debug.String(), "both")
}
