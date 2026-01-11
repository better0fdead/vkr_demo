package rafttransport

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	raftmembership "github.com/CanonicalLtd/raft-membership"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

type raftAddr struct {
	addr string
}

func (r *raftAddr) Network() string { return "tcp" }
func (r *raftAddr) String() string  { return r.addr }

func NewLayer(path string, addr string, handler *Handler, dial Dial, logger *zerolog.Logger) *Layer {
	localAddr := &raftAddr{addr: addr}
	return &Layer{
		path:      path,
		localAddr: localAddr,
		handler:   handler,
		dial:      dial,
		logger:    logger,
	}
}

type Layer struct {
	path      string
	localAddr *raftAddr
	handler   *Handler
	dial      Dial
	logger    *zerolog.Logger
}

func (l *Layer) Accept() (net.Conn, error) {
	select {
	case conn := <-l.handler.connections:
		return conn, nil
	case <-l.handler.shutdown:
		return nil, io.EOF
	}
}

func (l *Layer) Close() error {
	l.handler.Close()
	return nil
}

func (l *Layer) Addr() net.Addr {
	return l.localAddr
}

func (l *Layer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	l.logger.Debug().Msgf("raft-transport: Connecting to %s", addr)

	url, _ := url.Parse(l.path)
	request := &http.Request{
		Method:     http.MethodGet,
		URL:        url,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       l.Addr().String(),
	}
	request.Header.Set("Upgrade", "raft")

	conn, err := l.dial(string(addr), timeout)
	if err != nil {
		return nil, errors.Wrap(err, "dialing failed")
	}

	if err := request.Write(conn); err != nil {
		return nil, errors.Wrap(err, "sending HTTP request failed")
	}

	response, err := http.ReadResponse(bufio.NewReader(conn), request)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response")
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("dialing fail: expected status code 101 got %d", response.StatusCode)
	}
	if response.Header.Get("Upgrade") != "raft" {
		return nil, fmt.Errorf("missing or unexpected Upgrade header in response")
	}
	return conn, err
}

func (l *Layer) Join(id raft.ServerID, addr raft.ServerAddress, timeout time.Duration) error {
	l.logger.Debug().Msgf("raft-transport: Joining cluster at %s as node %s", addr, id)

	return l.changeMemberhip(raftmembership.JoinRequest, id, addr, timeout)
}

func (l *Layer) Leave(id raft.ServerID, addr raft.ServerAddress, timeout time.Duration) error {
	l.logger.Debug().Msgf("raft-transport: Leaving cluster at %s as node %s", addr, id)

	return l.changeMemberhip(raftmembership.LeaveRequest, id, addr, timeout)
}

func (l *Layer) changeMemberhip(kind raftmembership.ChangeRequestKind, id raft.ServerID, addr raft.ServerAddress, timeout time.Duration) error {
	return ChangeMembership(kind, l.path, l.dial, id, l.Addr().String(), string(addr), timeout)
}

var membershipChangeRequestKindToMethod = map[raftmembership.ChangeRequestKind]string{
	raftmembership.JoinRequest:  "POST",
	raftmembership.LeaveRequest: "DELETE",
}
