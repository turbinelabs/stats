package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

func startMockWavefrontProxy(port int) (chan *MockStat, error) {
	proxy := &mockProxy{
		port:  port,
		stats: make(chan *MockStat, 100),
	}

	if err := proxy.startConsumer(); err != nil {
		return nil, err
	}

	return proxy.stats, nil
}

type mockProxy struct {
	port       int
	listener   net.Listener
	stats      chan *MockStat
	chanClosed bool
}

func (p *mockProxy) open() error {
	host := fmt.Sprintf("0.0.0.0:%d", p.port)
	listener, err := net.Listen("tcp", host)
	if err != nil {
		return fmt.Errorf("could not start listener: %+v", err)
	}

	p.listener = listener

	return nil
}

func (p *mockProxy) consume(line string) error {
	parts := strings.SplitN(line, " ", 3)

	if len(parts) < 3 {
		return fmt.Errorf("'%s' does not contain enough fields to parse", line)
	}
	name := parts[0]

	value, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return fmt.Errorf("'%s' value is not legal: %+v", line, err)
	}
	tsAndTags := strings.SplitN(parts[2], " ", 2)

	var tagString string
	var timestamp time.Time
	ts, err := strconv.ParseInt(tsAndTags[0], 10, 64)
	if err != nil {
		timestamp = time.Now().Truncate(time.Second)
		tagString = strings.TrimSpace(parts[2])
	} else {
		timestamp = time.Unix(ts, 0)
		tagString = strings.TrimSpace(tsAndTags[1])
	}

	tags := map[string]string{}
	for tagString != "" {
		idx := strings.Index(tagString, "=")
		if idx < 0 {
			return fmt.Errorf("'%s' has invalid tags, cannot find '='", line)
		}

		key := tagString[0:idx]
		var value string
		if tagString[idx+1] == '"' {
			start := idx + 2
			end := strings.Index(tagString[start:], `"`)
			if end < 0 {
				return fmt.Errorf(
					"'%s' missing end quote (starts at %d)",
					line,
					idx+1,
				)
			}
			end += start
			value = tagString[start:end]
			tagString = tagString[end+1:]
		} else {
			start := idx + 1
			end := strings.Index(tagString[start:], " ")
			if end < 0 {
				value = tagString[start:]
				tagString = ""
			} else {
				end += start
				value = tagString[start:end]
				tagString = strings.TrimSpace(tagString[end+1:])
			}
		}

		tags[key] = value
	}

	p.stats <- &MockStat{Name: name, Value: value, Timestamp: timestamp, Tags: tags}

	return nil
}

func (p *mockProxy) startConsumer() error {
	if err := p.open(); err != nil {
		return err
	}

	go func() {
		for {
			c, err := p.listener.Accept()
			if err != nil {
				fmt.Printf("listener error: %+v\n", err)
				p.Close()
				return
			}

			go func(conn net.Conn) {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadString('\n')
					if err == io.EOF {
						break
					} else if err != nil {
						fmt.Printf("network error: %+v\n", err)
						p.Close()
						break
					}

					err = p.consume(strings.TrimSpace(line))
					if err != nil {
						fmt.Printf("parse error: %+v\n", err)
					}
				}
			}(c)
		}
	}()

	return nil
}

func (p *mockProxy) closeChannel() {
	if p.stats != nil && !p.chanClosed {
		close(p.stats)
		p.chanClosed = true
	}
}

func (p *mockProxy) Close() {
	p.closeChannel()

	if p.listener != nil {
		p.listener.Close()
		p.listener = nil
	}
}
