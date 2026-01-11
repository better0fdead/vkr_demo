package rafttransport

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	raftmembership "github.com/CanonicalLtd/raft-membership"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

type Handler struct {
	requests    chan *raftmembership.ChangeRequest
	connections chan net.Conn
	shutdown    chan struct{}
	timeout     time.Duration
	logger      *zerolog.Logger
	mu          sync.RWMutex
}

func NewHandler(logger *zerolog.Logger) *Handler {
	return &Handler{
		requests:    make(chan *raftmembership.ChangeRequest),
		connections: make(chan net.Conn),
		shutdown:    make(chan struct{}),
		timeout:     5 * time.Second,
		logger:      logger,
	}
}

func (h *Handler) Requests() <-chan *raftmembership.ChangeRequest {
	return h.requests
}

func (h *Handler) Timeout(timeout time.Duration) {
	h.timeout = timeout
}

func (h *Handler) Close() {
	close(h.shutdown)

	h.mu.Lock()
	defer h.mu.Unlock()

	close(h.requests)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/metrics" && r.Method == http.MethodGet {
		handler := promhttp.Handler()
		handler.ServeHTTP(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
	case http.MethodPost:
		h.handlePost(w, r)
	case http.MethodDelete:
		h.handleDelete(w, r)
	default:
		http.Error(w, "unknown action", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Upgrade") != "raft" {
		http.Error(w, "missing or invalid upgrade header", http.StatusBadRequest)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "webserver doesn't support hijacking", http.StatusInternalServerError)
		return
	}

	conn, _, err := hijacker.Hijack()
	if err != nil {
		message := errors.Wrap(err, "failed to hijack connection").Error()
		http.Error(w, message, http.StatusInternalServerError)
		return
	}

	data := []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: raft\r\n\r\n")
	if n, err := conn.Write(data); err != nil || n != len(data) {
		conn.Close()
		return
	}

	h.logger.Debug().Msgf("raft-transport: Establishing new connection with %s", r.Host)
	select {
	case <-h.shutdown:
		h.logger.Debug().Msgf("raft-transport: Connection from %s dropped since we have shutdowns", r.Host)
		conn.Close()
	case h.connections <- conn:
	case <-time.After(h.timeout):
		h.logger.Debug().Msgf("raft-transport: Connection from %s not processed within %s", r.Host, h.timeout)
		conn.Close()
	}
}

func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	id := raft.ServerID(query.Get("id"))
	address := raft.ServerAddress(query.Get("address"))

	h.logger.Debug().Msgf("raft-transport: Handling join request for node %s (%s)", id, address)

	request := raftmembership.NewJoinRequest(id, address)
	h.changeMembership(w, r, request)
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	id := raft.ServerID(query.Get("id"))

	h.logger.Debug().Msgf("raft-transport: Handling leave request for node %s", id)

	request := raftmembership.NewLeaveRequest(id)
	h.changeMembership(w, r, request)
}

func (h *Handler) changeMembership(w http.ResponseWriter, r *http.Request, request *raftmembership.ChangeRequest) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	select {
	case <-h.shutdown:
		http.Error(w, "raft transport closed", http.StatusForbidden)
		return
	default:
	}

	h.requests <- request

	err := request.Error(h.timeout)
	if err == nil {
		return
	}

	var code int

	switch err := err.(type) {
	case *raftmembership.ErrDifferentLeader:
		url := &url.URL{
			Scheme:   "http",
			Path:     r.URL.Path,
			RawQuery: r.URL.RawQuery,
			Host:     err.Leader(),
		}
		http.Redirect(w, r, url.String(), http.StatusPermanentRedirect)
		return
	case *raftmembership.ErrUnknownLeader:
		code = http.StatusServiceUnavailable
	default:
		code = http.StatusForbidden
	}

	message := errors.Wrap(err, fmt.Sprintf(
		"failed to %s server %s", request.Kind(), request.ID())).Error()
	http.Error(w, message, code)
}
