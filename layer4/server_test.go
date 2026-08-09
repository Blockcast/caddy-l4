// Copyright 2020 Matthew Holt
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package layer4

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
)

func TestServePacketStalledClientDoesNotBlockOtherClients(t *testing.T) {
	stalledAddr := &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 10001}
	healthyAddr := &net.UDPAddr{IP: net.ParseIP("192.0.2.2"), Port: 10002}
	stalledStarted := make(chan struct{})
	var stalledStartOnce sync.Once
	releaseStalled := make(chan struct{})
	var releaseStalledOnce sync.Once
	release := func() { releaseStalledOnce.Do(func() { close(releaseStalled) }) }
	healthyRead := make(chan string, 1)

	server := testPacketServer(t, HandlerFunc(func(cx *Connection) error {
		switch cx.RemoteAddr().String() {
		case stalledAddr.String():
			stalledStartOnce.Do(func() { close(stalledStarted) })
			<-releaseStalled
			return nil
		case healthyAddr.String():
			if err := cx.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				return err
			}
			buf := make([]byte, 32)
			n, err := cx.Read(buf)
			if err == nil {
				healthyRead <- string(buf[:n])
			}
			return err
		default:
			return nil
		}
	}))
	packetConn := newQueuedPacketConn(32)
	t.Cleanup(func() {
		release()
		_ = packetConn.Close()
	})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.servePacket(packetConn)
	}()

	packetConn.queue([]byte("stalled-0"), stalledAddr)
	waitForSignal(t, stalledStarted, "stalled client handler")

	// The per-client queue can hold five datagrams. Its first slot is already
	// occupied, so send enough to fill it and exercise the overflow path.
	for i := 0; i < udpClientQueueSize; i++ {
		packetConn.queue([]byte("stalled"), stalledAddr)
	}
	waitForPacketReads(t, packetConn, udpClientQueueSize+1)

	packetConn.queue([]byte("healthy"), healthyAddr)
	select {
	case got := <-healthyRead:
		if got != "healthy" {
			t.Fatalf("healthy client read %q, want %q", got, "healthy")
		}
	case <-time.After(time.Second):
		t.Fatal("healthy client was blocked behind a stalled client")
	}

	release()
	stopPacketServer(t, packetConn, serveDone)
}

func TestServePacketReaderKeepsDrainingUnderClientOverload(t *testing.T) {
	const packetCount = 4096

	stalledAddr := &net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 11001}
	healthyAddr := &net.UDPAddr{IP: net.ParseIP("192.0.2.11"), Port: 11002}
	stalledStarted := make(chan struct{})
	var stalledStartOnce sync.Once
	releaseStalled := make(chan struct{})
	var releaseStalledOnce sync.Once
	release := func() { releaseStalledOnce.Do(func() { close(releaseStalled) }) }
	healthyRead := make(chan string, 1)

	server := testPacketServer(t, HandlerFunc(func(cx *Connection) error {
		switch cx.RemoteAddr().String() {
		case stalledAddr.String():
			stalledStartOnce.Do(func() { close(stalledStarted) })
			<-releaseStalled
			return nil
		case healthyAddr.String():
			if err := cx.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				return err
			}
			buf := make([]byte, 32)
			n, err := cx.Read(buf)
			if err == nil {
				healthyRead <- string(buf[:n])
			}
			return err
		default:
			return nil
		}
	}))
	packetConn := newQueuedPacketConn(packetCount + 1)
	t.Cleanup(func() {
		release()
		_ = packetConn.Close()
	})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.servePacket(packetConn)
	}()

	packetConn.queue([]byte("first"), stalledAddr)
	waitForSignal(t, stalledStarted, "stalled client handler")
	for i := 1; i < packetCount; i++ {
		packetConn.queue([]byte("overload"), stalledAddr)
		if i == packetCount/2 {
			packetConn.queue([]byte("healthy"), healthyAddr)
		}
	}

	select {
	case got := <-healthyRead:
		if got != "healthy" {
			t.Fatalf("healthy client read %q, want %q", got, "healthy")
		}
	case <-time.After(time.Second):
		t.Fatal("healthy client was starved during sustained stalled-client traffic")
	}

	// ReadFrom represents draining the kernel socket receive queue. A saturated
	// per-client queue must shed that client's datagrams instead of stopping
	// socket reads and allowing the shared kernel Recv-Q to grow.
	waitForPacketReads(t, packetConn, packetCount+1)
	if got := testutil.ToFloat64(server.metrics.udpPacketsDropped); got == 0 {
		t.Fatal("overloaded virtual connection did not record dropped datagrams")
	}

	release()
	stopPacketServer(t, packetConn, serveDone)
}

func TestRemovePacketConnIgnoresStaleGeneration(t *testing.T) {
	addr := &net.UDPAddr{IP: net.ParseIP("192.0.2.20"), Port: 12001}
	stale := &packetConn{addr: addr, readCh: make(chan *packet, 1)}
	replacement := &packetConn{addr: addr, readCh: make(chan *packet, 1)}
	udpConns := map[string]*packetConn{addr.String(): replacement}

	removePacketConn(udpConns, stale)
	if got := udpConns[addr.String()]; got != replacement {
		t.Fatal("stale close removed the replacement connection")
	}
	select {
	case _, ok := <-replacement.readCh:
		if !ok {
			t.Fatal("stale close closed the replacement connection's read queue")
		}
	default:
	}
}

func TestPacketConnCloseNotificationIsIdempotent(t *testing.T) {
	notifications := make(chan *packetConn, 2)
	done := make(chan struct{})
	conn := &packetConn{
		addr:    &net.UDPAddr{IP: net.ParseIP("192.0.2.21"), Port: 12002},
		readCh:  make(chan *packet, 1),
		closeCh: notifications,
		done:    done,
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := len(notifications); got != 1 {
		t.Fatalf("close notifications = %d, want 1", got)
	}
	if got := <-notifications; got != conn {
		t.Fatal("close notification did not preserve connection generation")
	}
}

func TestServerMetricsReuseDropCounter(t *testing.T) {
	registry := prometheus.NewRegistry()
	first, err := newServerMetrics(registry)
	if err != nil {
		t.Fatalf("first metrics registration: %v", err)
	}
	second, err := newServerMetrics(registry)
	if err != nil {
		t.Fatalf("second metrics registration: %v", err)
	}
	first.packetDropped()
	second.packetDropped()

	if got := testutil.ToFloat64(first.udpPacketsDropped); got != 2 {
		t.Fatalf("shared UDP drop counter = %v, want 2", got)
	}
}

func TestServerMetricsRejectMismatchedCollector(t *testing.T) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "caddy",
		Subsystem: "layer4",
		Name:      "udp_packets_dropped_total",
		Help:      "Number of UDP datagrams dropped because a virtual connection's bounded queue was full.",
	}))

	if _, err := newServerMetrics(registry); err == nil {
		t.Fatal("mismatched existing UDP drop collector was accepted")
	}
}

func TestServePacketTerminalErrorClosesVirtualConnections(t *testing.T) {
	addr := &net.UDPAddr{IP: net.ParseIP("192.0.2.22"), Port: 12003}
	handlerStarted := make(chan struct{})
	handlerDone := make(chan error, 1)

	server := testPacketServer(t, HandlerFunc(func(cx *Connection) error {
		if err := cx.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		close(handlerStarted)
		buf := make([]byte, 32)
		if _, err := cx.Read(buf); err != nil {
			handlerDone <- err
			return err
		}
		_, err := cx.Read(buf)
		handlerDone <- err
		return err
	}))
	packetConn := newQueuedPacketConn(1)
	t.Cleanup(func() { _ = packetConn.Close() })
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.servePacket(packetConn)
	}()

	packetConn.queue([]byte("first"), addr)
	waitForSignal(t, handlerStarted, "virtual connection handler")
	stopPacketServer(t, packetConn, serveDone)
	select {
	case err := <-handlerDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("virtual connection ended with %v, want io.EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("virtual connection remained blocked after packet socket exit")
	}
}

func testPacketServer(t *testing.T, handler Handler) *Server {
	t.Helper()
	metrics, err := newServerMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("creating test server metrics: %v", err)
	}
	return &Server{
		compiledRoute: handler,
		logger:        zap.NewNop(),
		IdleTimeout:   caddy.Duration(idleTimeoutDefault),
		metrics:       metrics,
	}
}

func waitForSignal(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForPacketReads(t *testing.T, packetConn *queuedPacketConn, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := packetConn.reads.Load(); got >= int64(want) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("socket reader consumed %d datagrams, want %d", packetConn.reads.Load(), want)
}

func stopPacketServer(t *testing.T, packetConn *queuedPacketConn, serveDone <-chan error) {
	t.Helper()
	if err := packetConn.Close(); err != nil {
		t.Fatalf("closing packet connection: %v", err)
	}
	select {
	case err := <-serveDone:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("servePacket returned %v, want net.ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("servePacket did not stop after closing its packet connection")
	}
}

type queuedDatagram struct {
	payload []byte
	addr    net.Addr
}

type queuedPacketConn struct {
	incoming chan queuedDatagram
	closed   chan struct{}
	close    sync.Once
	reads    atomic.Int64
	local    net.Addr
}

func newQueuedPacketConn(capacity int) *queuedPacketConn {
	return &queuedPacketConn{
		incoming: make(chan queuedDatagram, capacity),
		closed:   make(chan struct{}),
		local:    &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 443},
	}
}

func (pc *queuedPacketConn) queue(payload []byte, addr net.Addr) {
	pc.incoming <- queuedDatagram{payload: payload, addr: addr}
}

func (pc *queuedPacketConn) ReadFrom(buf []byte) (int, net.Addr, error) {
	select {
	case datagram := <-pc.incoming:
		pc.reads.Add(1)
		return copy(buf, datagram.payload), datagram.addr, nil
	case <-pc.closed:
		return 0, nil, net.ErrClosed
	}
}

func (pc *queuedPacketConn) WriteTo(buf []byte, _ net.Addr) (int, error) {
	return len(buf), nil
}

func (pc *queuedPacketConn) Close() error {
	pc.close.Do(func() {
		close(pc.closed)
	})
	return nil
}

func (pc *queuedPacketConn) LocalAddr() net.Addr { return pc.local }

func (pc *queuedPacketConn) SetDeadline(time.Time) error { return nil }

func (pc *queuedPacketConn) SetReadDeadline(time.Time) error { return nil }

func (pc *queuedPacketConn) SetWriteDeadline(time.Time) error { return nil }
