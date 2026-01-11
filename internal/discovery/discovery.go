package discovery

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/quic-go/quic-go/http3"
	"github.com/rs/zerolog/log"
)

type NodeInfo struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raftAddr"`
	Addr     string `json:"addr"`
}

type Option func(*QuicDiscovery)

func WithAdvertiseAddress(addr string) Option {
	return func(d *QuicDiscovery) {
		d.advertiseAddr = addr
	}
}

type QuicDiscovery struct {
	nodeID        string
	raftAddr      string
	advertiseAddr string
	Addr          string
	nodes         map[string]*NodeInfo
	nodesMu       sync.RWMutex
	joinHandler   func(string, string) error
	leaveHandler  func(string) error
	client        *http3.Transport
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

type DiscoveryMessage struct {
	Type     string     `json:"type"`
	NodeID   string     `json:"node_id"`
	RaftAddr string     `json:"raft_addr"`
	Addr     string     `json:"addr"`
	Nodes    []NodeInfo `json:"nodes,omitempty"`
}

func NewQuicDiscovery(nodeID, raftAddr string, addr string, tlsConfig *tls.Config, joinHandler func(string, string) error, leaveHandler func(string) error, options ...Option) (*QuicDiscovery, error) {
	host, _, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid raft address format: %v", err)
	}

	d := &QuicDiscovery{
		nodeID:        nodeID,
		raftAddr:      raftAddr,
		advertiseAddr: host,
		Addr:          addr,
		nodes:         make(map[string]*NodeInfo),
		joinHandler:   joinHandler,
		leaveHandler:  leaveHandler,
		stopCh:        make(chan struct{}),
	}

	for _, opt := range options {
		opt(d)
	}

	d.client = &http3.Transport{
		TLSClientConfig: tlsConfig,
	}

	return d, nil
}

func (d *QuicDiscovery) Start() error {
	d.nodesMu.Lock()
	d.nodes[d.nodeID] = &NodeInfo{
		ID:       d.nodeID,
		RaftAddr: d.raftAddr,
		Addr:     d.Addr,
	}
	d.nodesMu.Unlock()

	d.wg.Add(1)
	go d.heartbeatLoop()

	return nil
}

func (d *QuicDiscovery) Stop() error {
	close(d.stopCh)
	d.wg.Wait()
	return nil
}

func (d *QuicDiscovery) Join(seeds []string) (int, error) {
	if len(seeds) == 0 {
		return 0, fmt.Errorf("no seed addresses provided")
	}

	joined := 0
	var lastErr error

	for _, seed := range seeds {
		if err := d.joinViaNode(seed); err != nil {
			log.Warn().Err(err).Str("seed", seed).Msg("Failed to join via seed")
			lastErr = err
			continue
		}
		joined++
		break
	}

	if joined == 0 && lastErr != nil {
		return 0, fmt.Errorf("failed to join: %v", lastErr)
	}

	return joined, nil
}

func (d *QuicDiscovery) joinViaNode(nodeAddr string) error {
	msg := DiscoveryMessage{
		Type:     "JOIN",
		NodeID:   d.nodeID,
		RaftAddr: d.raftAddr,
		Addr:     d.Addr,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal join message: %v", err)
	}

	url := fmt.Sprintf("https://%s/discovery", nodeAddr)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Transport: d.client,
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send join request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("join request failed with status: %d", resp.StatusCode)
	}

	var response DiscoveryMessage
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode response: %v", err)
	}

	d.nodesMu.Lock()
	for _, node := range response.Nodes {
		if node.ID != d.nodeID {
			d.nodes[node.ID] = &NodeInfo{
				ID:       node.ID,
				RaftAddr: node.RaftAddr,
				Addr:     node.Addr,
			}

			if d.joinHandler != nil {
				if err := d.joinHandler(node.ID, node.RaftAddr); err != nil {
					log.Error().Err(err).
						Str("node_id", node.ID).
						Str("raft_addr", node.RaftAddr).
						Msg("Failed to handle node join")
				}
			}
		}
	}
	d.nodesMu.Unlock()

	return nil
}

func (d *QuicDiscovery) heartbeatLoop() {
	defer d.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.sendHeartbeats()
		case <-d.stopCh:
			return
		}
	}
}

func (d *QuicDiscovery) sendHeartbeats() {
	d.nodesMu.RLock()
	nodes := make(map[string]*NodeInfo, len(d.nodes))
	for id, node := range d.nodes {
		nodes[id] = node
	}
	d.nodesMu.RUnlock()

	msg := DiscoveryMessage{
		Type:     "HEARTBEAT",
		NodeID:   d.nodeID,
		RaftAddr: d.raftAddr,
		Addr:     d.Addr,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal heartbeat message")
		return
	}

	client := &http.Client{
		Transport: d.client,
		Timeout:   3 * time.Second,
	}

	for id, node := range nodes {
		if id == d.nodeID {
			continue
		}

		url := fmt.Sprintf("https://%s/discovery", node.Addr)
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			log.Warn().Err(err).Str("node", id).Msg("Failed to create heartbeat request")
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		go func(nodeID string, req *http.Request) {
			resp, err := client.Do(req)
			if err != nil {
				log.Warn().Err(err).Str("node", id).Msg("Failed to send heartbeat request")
				d.handleNodeFailure(nodeID)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Warn().Int("status", resp.StatusCode).Str("node", nodeID).Msg("Heartbeat received non-OK response")
			}
		}(id, req)
	}
}

func (d *QuicDiscovery) handleNodeFailure(nodeID string) {
	d.nodesMu.Lock()
	defer d.nodesMu.Unlock()

	_, exists := d.nodes[nodeID]
	if !exists {
		return
	}

	if d.leaveHandler != nil {
		if err := d.leaveHandler(nodeID); err != nil {
			log.Error().Err(err).Str("node_id", nodeID).Msg("Failed to handle node leave")
		}
	}

	delete(d.nodes, nodeID)
	log.Info().Str("node_id", nodeID).Msg("Node removed from cluster after failure")
}

func (d *QuicDiscovery) HandleDiscoveryMessage(message []byte) ([]byte, error) {
	var msg DiscoveryMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal discovery message: %v", err)
	}

	switch msg.Type {
	case "JOIN":
		return d.handleJoinMessage(msg)
	case "HEARTBEAT":
		return d.handleHeartbeatMessage(msg)
	default:
		return nil, fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

func (d *QuicDiscovery) handleJoinMessage(msg DiscoveryMessage) ([]byte, error) {
	d.nodesMu.Lock()
	d.nodes[msg.NodeID] = &NodeInfo{
		ID:       msg.NodeID,
		RaftAddr: msg.RaftAddr,
		Addr:     msg.Addr,
	}
	d.nodesMu.Unlock()

	if d.joinHandler != nil {
		if err := d.joinHandler(msg.NodeID, msg.RaftAddr); err != nil {
			log.Error().Err(err).
				Str("node_id", msg.NodeID).
				Str("raft_addr", msg.RaftAddr).
				Msg("Failed to handle node join")
		}
	}

	d.nodesMu.RLock()
	nodesList := make([]NodeInfo, 0, len(d.nodes))
	for _, node := range d.nodes {
		nodesList = append(nodesList, *node)
	}
	d.nodesMu.RUnlock()

	response := DiscoveryMessage{
		Type:  "JOIN_RESPONSE",
		Nodes: nodesList,
	}

	return json.Marshal(response)
}

func (d *QuicDiscovery) handleHeartbeatMessage(msg DiscoveryMessage) ([]byte, error) {
	d.nodesMu.Lock()
	if _, exists := d.nodes[msg.NodeID]; !exists {
		d.nodes[msg.NodeID] = &NodeInfo{
			ID:       msg.NodeID,
			RaftAddr: msg.RaftAddr,
			Addr:     msg.Addr,
		}

		if d.joinHandler != nil {
			if err := d.joinHandler(msg.NodeID, msg.RaftAddr); err != nil {
				log.Error().Err(err).
					Str("node_id", msg.NodeID).
					Str("raft_addr", msg.RaftAddr).
					Msg("Failed to handle node join from heartbeat")
			}
		}
	} else {
		node := d.nodes[msg.NodeID]
		if node.Addr != msg.Addr && msg.Addr != "" {
			node.Addr = msg.Addr
			log.Debug().
				Str("node_id", msg.NodeID).
				Str("http3_addr", msg.Addr).
				Msg("Updated node HTTP3 address")
		}
	}
	d.nodesMu.Unlock()

	response := DiscoveryMessage{
		Type:   "HEARTBEAT_ACK",
		NodeID: d.nodeID,
		Addr:   d.Addr,
	}

	return json.Marshal(response)
}

func (d *QuicDiscovery) GetNodes() []NodeInfo {
	d.nodesMu.RLock()
	defer d.nodesMu.RUnlock()

	nodes := make([]NodeInfo, 0, len(d.nodes))
	for _, node := range d.nodes {
		nodes = append(nodes, *node)
	}

	return nodes
}
