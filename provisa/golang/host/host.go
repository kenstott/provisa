// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8a1d27-5c94-4e0b-a6d2-91e7b4c0f538
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The host a Go source's worker runs (REQ-2200): the protocol of every code source
// (provisa/codesource/worker.py), in Go.
//
//	host <socket path | fd:N[,M...]>
//
// Unlike Node's and Python's hosts, which import the code when they start, this one is compiled
// with the code's package linked in (provisa/golang/build.py): one static binary per code hash,
// whose generated exports.go registers each tagged function. It connects to the socket
// PROVISA_HOST_CONNECTIONS times (or takes the connections it inherited, already connected),
// says on each which functions it has, and serves every connection's calls until one closes,
// when it exits. Each call runs on a goroutine of its own, with a context cancelled when Provisa
// cancels the call: a function that takes a context.Context first is given it.
//
// Rows go as Arrow IPC batches against the schema the call gives (REQ-2098,
// provisa/codesource/arrow_rows.py), built with Apache Arrow Go; a table's in batches of 1000, a
// stream's each as it is yielded. What the code prints goes to stderr, which Provisa keeps to
// name a crash.
package main

import (
	"bufio"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

// The protocol this host speaks (provisa/codesource/worker.py PROTOCOL).
const protocol = 4

const (
	connectionsEnv = "PROVISA_HOST_CONNECTIONS"
	codeHashEnv    = "PROVISA_CODE_HASH"
	// A PEM bundle of the certificates TLS is verified with, where the system's own verifier is
	// out of the sandbox's reach (macOS, whose trust service is not one the sandbox gives).
	certificatesEnv = "PROVISA_CERTIFICATES"
	jsonFrame       = 'J'
	arrowFrame      = 'A'
)

// refused is Provisa refusing this host: its protocol or its code is not the source's.
type refused struct{ message string }

// connection is one of the host's connections to Provisa, and the calls running on it.
type connection struct {
	conn    net.Conn
	reader  *bufio.Reader
	writing sync.Mutex
	calls   sync.Map // call id -> *call
}

func (c *connection) frame(kind byte, body []byte) error {
	head := make([]byte, 5)
	binary.BigEndian.PutUint32(head, uint32(len(body)+1))
	head[4] = kind
	c.writing.Lock()
	defer c.writing.Unlock()
	if _, err := c.conn.Write(head); err != nil {
		return err
	}
	_, err := c.conn.Write(body)
	return err
}

func (c *connection) send(message map[string]any) error {
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return c.frame(jsonFrame, body)
}

func (c *connection) sendRows(id uint64, ipc []byte) error {
	body := make([]byte, 8, 8+len(ipc))
	binary.BigEndian.PutUint64(body, id)
	return c.frame(arrowFrame, append(body, ipc...))
}

func (c *connection) read() (map[string]any, error) {
	head := make([]byte, 4)
	if _, err := io.ReadFull(c.reader, head); err != nil {
		return nil, err
	}
	body := make([]byte, binary.BigEndian.Uint32(head))
	if _, err := io.ReadFull(c.reader, body); err != nil {
		return nil, err
	}
	if len(body) == 0 || body[0] != jsonFrame {
		return nil, fmt.Errorf("a frame of unknown kind")
	}
	var message map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(body[1:])))
	decoder.UseNumber()
	if err := decoder.Decode(&message); err != nil {
		return nil, err
	}
	return message, nil
}

func number(value any) int64 {
	n, _ := value.(json.Number).Int64()
	return n
}

// serve says hello on the connection and serves its calls until it closes.
func (c *connection) serve() error {
	if err := c.send(map[string]any{
		"op":        "hello",
		"protocol":  protocol,
		"code_hash": os.Getenv(codeHashEnv),
		"exports":   exportNames(),
	}); err != nil {
		return err
	}
	for {
		message, err := c.read()
		if err != nil {
			c.calls.Range(func(_, running any) bool {
				running.(*call).cancel()
				return true
			})
			return nil // the socket closed: Provisa stopped the worker
		}
		switch message["op"] {
		case "refused":
			return refused{fmt.Sprint(message["message"])}
		case "ping":
			if err := c.send(map[string]any{"op": "pong", "id": number(message["id"])}); err != nil {
				return err
			}
		case "cancel":
			if running, ok := c.calls.Load(uint64(number(message["id"]))); ok {
				running.(*call).cancel()
			}
		case "credit":
			if running, ok := c.calls.Load(uint64(number(message["id"]))); ok {
				running.(*call).credit(int(number(message["n"])))
			}
		case "call":
			started := newCall(c, message)
			c.calls.Store(started.id, started)
			go started.run()
		}
	}
}

func (r refused) Error() string { return r.message }

func connections(address string) ([]net.Conn, error) {
	if descriptors, ok := strings.CutPrefix(address, "fd:"); ok {
		// The Linux sandbox refuses the host any new socket but TCP: Provisa hands it these.
		var out []net.Conn
		for _, fd := range strings.Split(descriptors, ",") {
			n, err := strconv.Atoi(fd)
			if err != nil {
				return nil, fmt.Errorf("fd:%s is not a descriptor", fd)
			}
			file := os.NewFile(uintptr(n), "provisa-"+fd)
			conn, err := net.FileConn(file)
			if err != nil {
				return nil, err
			}
			file.Close()
			out = append(out, conn)
		}
		return out, nil
	}
	count, err := strconv.Atoi(os.Getenv(connectionsEnv))
	if err != nil {
		return nil, fmt.Errorf("%s is not a number of connections", connectionsEnv)
	}
	out := make([]net.Conn, count)
	for i := range out {
		if out[i], err = net.Dial("unix", address); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// certificates makes the bundle PROVISA_CERTIFICATES names the roots every TLS connection is
// verified with (GODEBUG=x509usefallbackroots=1 is set beside it).
func certificates() error {
	file := os.Getenv(certificatesEnv)
	if file == "" {
		return nil
	}
	pem, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return fmt.Errorf("%s holds no certificate", file)
	}
	x509.SetFallbackRoots(roots)
	return nil
}

func run(address string) error {
	if err := certificates(); err != nil {
		return err
	}
	conns, err := connections(address)
	if err != nil {
		return err
	}
	done := make(chan error, len(conns))
	for _, conn := range conns {
		served := &connection{conn: conn, reader: bufio.NewReaderSize(conn, 1<<16)}
		go func() { done <- served.serve() }()
	}
	// One connection closing is Provisa stopping the worker: the others' calls end with it.
	return <-done
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: host <socket path | fd:N[,M...]>")
		os.Exit(2)
	}
	// What the code prints goes to stderr; the protocol is the socket's alone.
	os.Stdout = os.Stderr
	if err := run(os.Args[1]); err != nil {
		if r, ok := err.(refused); ok {
			fmt.Fprintf(os.Stderr, "provisa refused this host: %s\n", r.message)
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0) // goroutines still running a cancelled call do not hold the process open
}
