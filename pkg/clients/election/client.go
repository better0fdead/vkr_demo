package election

import (
	"context"
	"crypto/tls"
	"net"

	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/cluster"
	server "gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/quic"
)

const (
	initialRetry    = 1 * time.Second
	serverAddr      = "https://election.portal.wb.ru:9000/stream"
	retryInterval   = 30 * time.Second
	electionTimeout = 2 * time.Second
	heartbeatRate   = 500 * time.Millisecond
)

type LeaderEvent struct {
	Group         string
	Leader        string
	IsLeader      bool
	LeaderChanged bool
}

type LeaderCallbacks struct {
	OnImLeader      func(event LeaderEvent) // when this client becomes leader
	OnImNotLeader   func(event LeaderEvent) // when this client loses leadership
	OnLeaderChanged func(event LeaderEvent) // when the group's leader has changed
}

type Client struct {
	addr    string
	tlsConf *tls.Config
	rt      *http3.Transport
	log     *zerolog.Logger

	group           string
	member          string
	connected       bool
	retryInterval   time.Duration
	electionTimeout time.Duration
	heartbeatRate   time.Duration
	leaderCallbacks LeaderCallbacks

	reconnectCh chan struct{}
	ctx         context.Context
	cancelFunc  context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.Mutex
	closed      bool
}

func NewClient(group string, opts ...Option) (*Client, error) {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h3"},
	}

	hostname, _ := os.Hostname()

	ctx, cancel := context.WithCancel(context.Background())
	level := zerolog.InfoLevel

	out := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.StampMicro}

	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	logger := zerolog.New(out).Level(level).With().Timestamp().Stack()

	log := logger.Logger().With().Str("client", group).Logger()

	c := &Client{
		addr:            serverAddr,
		tlsConf:         tlsConf,
		reconnectCh:     make(chan struct{}, 1),
		group:           group,
		member:          hostname,
		retryInterval:   retryInterval,
		electionTimeout: electionTimeout,
		leaderCallbacks: LeaderCallbacks{
			OnImLeader:      func(LeaderEvent) {},
			OnImNotLeader:   func(LeaderEvent) {},
			OnLeaderChanged: func(LeaderEvent) {},
		},
		rt: &http3.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			QUICConfig: &quic.Config{
				MaxIdleTimeout: time.Minute,
			},
		},
		connected:     false,
		ctx:           ctx,
		cancelFunc:    cancel,
		closed:        false,
		heartbeatRate: heartbeatRate,
		log:           &log,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

func (c *Client) CreateStream(ctx context.Context) (http3.RequestStream, error) {
	req, err := http.NewRequest(http.MethodGet, c.addr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	c.tlsConf.NextProtos = []string{http3.NextProtoH3}
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return nil, err
	}

	parsedURL, err := url.Parse(c.addr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	udpAddr := parsedURL.Host
	udpAddrv, err := net.ResolveUDPAddr("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	conn, err := quic.Dial(ctx, udpConn, udpAddrv, c.tlsConf, &quic.Config{
		MaxIdleTimeout: time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to dial QUIC connection: %w", err)
	}

	cc := c.rt.NewClientConn(conn)

	stream, err := cc.OpenRequestStream(ctx)
	if err != nil {
		err = conn.CloseWithError(0, "failed to open request stream")
		return nil, fmt.Errorf("failed to open request stream: %w", err)
	}

	if err := stream.SendRequestHeader(req); err != nil {
		stream.Close()
		return nil, fmt.Errorf("failed to send request headers: %w", err)
	}

	resp, err := stream.ReadResponse()
	if resp.StatusCode != http.StatusOK || err != nil {
		stream.Close()
		return nil, fmt.Errorf("failed to read respose: %w", err)
	}

	b := make([]byte, 9)
	_, err = io.ReadFull(stream, b)
	if err != nil {
		stream.Close()
		return nil, fmt.Errorf("failed to read respose: %w", err)
	}

	return stream, nil
}

func (c *Client) RegisterCallbacks(callbacks LeaderCallbacks) {
	c.leaderCallbacks = callbacks
}

func (c *Client) tryJoinGroup(group, member string) error {
	if c.isClosed() {
		return fmt.Errorf("client is closed")
	}

	stream, err := c.CreateStream(c.ctx)
	if err != nil {
		return err
	}

	go func() {
		<-stream.Context().Done()
	}()

	c.joinGroup(stream, group, member)

	go c.readLoop(stream, group, member)

	go c.heartbeatLoop(stream, group, member)

	if !c.isClosed() {
		c.setConnected(true)
	}

	return nil
}

func (c *Client) joinGroup(stream http3.RequestStream, group, member string) {
	config := cluster.GroupConfig{ElectionTimeout: c.electionTimeout}
	configData, _ := json.Marshal(config)
	msg := server.Message{
		Op:     "JN",
		Group:  group,
		Member: member,
		Config: configData,
	}

	if err := json.NewEncoder(stream).Encode(msg); err != nil {
		c.log.Warn().Err(err).Msgf("Failed to send join message: %v", err)
	}

}

func (c *Client) heartbeatLoop(stream http3.RequestStream, group, member string) {
	ticker := time.NewTicker(c.heartbeatRate)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.isClosed() {
				return
			}

			msg := server.Message{
				Op:     "HB",
				Group:  group,
				Member: member,
			}

			if err := json.NewEncoder(stream).Encode(msg); err != nil {
				c.log.Warn().Err(err).Msgf("Failed to send heartbeat: %v", err)
				return
			}
		case <-stream.Context().Done():
			return
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) readLoop(stream http3.RequestStream, group, member string) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Warn().Msgf("Recovered from panic in readLoop: %v", r)
		}
	}()

	var previousLeader string
	var wasLeader bool
	var response server.Response
	var redirectResponse server.RedirectResponse

	decoder := json.NewDecoder(stream)
	for {
		select {
		case <-stream.Context().Done():
			c.log.Info().Msgf("Stream context canceled")
			return
		case <-c.ctx.Done():
			return
		default:
			var rawMsg map[string]interface{}
			if err := decoder.Decode(&rawMsg); err != nil {
				if c.isClosed() {
					c.log.Info().Msgf("Client is closed, not attempting to reconnect")
					return
				}

				if err == io.EOF {
					c.log.Info().Msgf("Connection closed by server")
					c.setConnected(false)
					if !c.isClosed() {
						select {
						case c.reconnectCh <- struct{}{}:
						default:
						}
					}
					return
				}
				if strings.Contains(err.Error(), "client shutdown") {
					c.log.Info().Msgf("Client shutdown gracefully")
					return
				}
				c.log.Warn().Err(err).Msgf("Read error: %v", err)
				c.setConnected(false)
				if !c.isClosed() {
					select {
					case c.reconnectCh <- struct{}{}:
					default:
					}
				}
				return
			}

			msgType, ok := rawMsg["type"].(string)
			if !ok {
				c.log.Warn().Msgf("Invalid message format, missing 'type' field: %v", rawMsg)
				continue
			}
			switch msgType {
			case "REDIRECT":
				msgBytes, err := json.Marshal(rawMsg)
				if err != nil {
					c.log.Warn().Err(err).Msgf("Failed to re-marshal raw message: %v", err)
					continue
				}

				if err := json.Unmarshal(msgBytes, &redirectResponse); err != nil {
					c.log.Warn().Err(err).Msgf("Failed to unmarshal redirect response: %v", err)
					continue
				}

				c.log.Info().Msgf("Received redirect to leader at: %s", redirectResponse.LeaderAddr)

				c.mu.Lock()
				oldAddr := c.addr
				newAddr := fmt.Sprintf("https://%s/stream", redirectResponse.LeaderAddr)
				c.addr = newAddr
				c.mu.Unlock()

				c.log.Info().Msgf("Redirecting from %s to %s", oldAddr, newAddr)

				stream.Close()
				c.setConnected(false)

				if !c.isClosed() {
					select {
					case c.reconnectCh <- struct{}{}:
						c.log.Info().Msgf("Reconnect scheduled")
					default:
						c.log.Info().Msgf("Reconnect already scheduled")
					}
				}
				return
			case "LEADER":
				msgBytes, err := json.Marshal(rawMsg)
				if err != nil {
					c.log.Warn().Err(err).Msgf("Failed to re-marshal raw message: %v", err)
					continue
				}

				if err := json.Unmarshal(msgBytes, &response); err != nil {
					c.log.Warn().Err(err).Msgf("Failed to unmarshal leader response: %v", err)
					continue
				}

				currentLeader := response.Leader
				imLeader := response.IsLeader
				event := LeaderEvent{
					Group:         group,
					Leader:        currentLeader,
					IsLeader:      imLeader,
					LeaderChanged: previousLeader != currentLeader,
				}

				if c.leaderCallbacks.OnLeaderChanged != nil && previousLeader != currentLeader {
					c.leaderCallbacks.OnLeaderChanged(event)
				}

				if c.leaderCallbacks.OnImLeader != nil && imLeader && !wasLeader {
					c.leaderCallbacks.OnImLeader(event)
				}

				if c.leaderCallbacks.OnImNotLeader != nil && !imLeader && wasLeader {
					c.leaderCallbacks.OnImNotLeader(event)
				}

				previousLeader = currentLeader
				wasLeader = imLeader
			default:
				c.log.Warn().Msgf("Received unknown message type: %s", msgType)
			}
		}
	}
}

func (c *Client) setConnected(state bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.connected = state
	}
}

func (c *Client) isConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected && !c.closed
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Client) Start() error {
	if c.isClosed() {
		return fmt.Errorf("cannot start a closed client")
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		err := c.tryJoinGroup(c.group, c.member)
		if err != nil {
			c.log.Warn().Err(err).Msgf("Failed to join group: %v", err)
		}
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.reconnectionMonitor()
	}()

	return nil
}

func (c *Client) reconnectionMonitor() {
	ticker := time.NewTicker(c.retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !c.isConnected() && !c.isClosed() {
				c.log.Info().Msgf("Attempting to reconnect...")
				if err := c.tryReconnect(); err != nil {
					c.log.Warn().Err(err).Msgf("Reconnection failed: %v", err)
				}
			}
		case <-c.reconnectCh:
			if c.isClosed() {
				c.log.Warn().Msgf("Client is closed, ignoring reconnect request")
				return
			}

			c.log.Warn().Msgf("Connection lost, attempting to reconnect...")
			if err := c.tryReconnect(); err != nil {
				c.log.Warn().Err(err).Msgf("Reconnection failed: %v", err)
			}
		case <-c.ctx.Done():
			c.log.Info().Msgf("Reconnection monitor shutting down")
			return
		}
	}
}

func (c *Client) tryReconnect() error {
	if c.isConnected() || c.isClosed() {
		return nil
	}

	stream, err := c.CreateStream(c.ctx)
	if err != nil {
		return err
	}

	c.joinGroup(stream, c.group, c.member)

	go func() {
		<-stream.Context().Done()
	}()

	go c.heartbeatLoop(stream, c.group, c.member)

	go c.readLoop(stream, c.group, c.member)

	if !c.isClosed() {
		c.setConnected(true)
		c.log.Info().Msgf("Successfully reconnected to server")
	} else {
		stream.Close()
		return fmt.Errorf("client is closed")
	}

	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}

	c.closed = true
	c.connected = false
	c.mu.Unlock()

	c.log.Info().Msgf("Closing client...")

	c.cancelFunc()

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stream, err := c.CreateStream(ctx)
		if err == nil {
			defer stream.Close()

			msg := server.Message{
				Op:     "LV",
				Group:  c.group,
				Member: c.member,
			}

			if err := json.NewEncoder(stream).Encode(msg); err != nil {
				c.log.Warn().Err(err).Msgf("Failed to send leave message: %v", err)
			} else {
				c.log.Info().Msgf("Successfully sent leave message to server")
			}
		} else {
			c.log.Warn().Err(err).Msgf("Failed to create stream for leave message: %v", err)
		}
	}()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.log.Info().Msgf("All goroutines finished")
	case <-time.After(10 * time.Second):
		c.log.Warn().Msgf("imed out waiting for goroutines to finish")
	}

	if err := c.rt.Close(); err != nil {
		c.log.Warn().Err(err).Msgf("Error closing RoundTripper: %v", err)
		return fmt.Errorf("failed to close RoundTripper: %w", err)
	}

	for {
		select {
		case <-c.reconnectCh:
		default:
			close(c.reconnectCh)
			c.log.Info().Msgf("Client closed successfully")
			return nil
		}
	}
}

func (c *Client) GetGroupState(group string) (cluster.GroupState, error) {
	if c.isClosed() {
		return cluster.GroupState{}, fmt.Errorf("client is closed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := c.CreateStream(ctx)
	if err != nil {
		return cluster.GroupState{}, fmt.Errorf("failed to create stream: %w", err)
	}
	defer stream.Close()

	msg := server.Message{
		Op:    "GET_STATE",
		Group: group,
	}

	if err := json.NewEncoder(stream).Encode(msg); err != nil {
		return cluster.GroupState{}, fmt.Errorf("failed to send get state request: %w", err)
	}

	err = stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		return cluster.GroupState{}, err
	}

	var response server.ResponseState

	if err := json.NewDecoder(stream).Decode(&response); err != nil {
		return cluster.GroupState{}, fmt.Errorf("failed to decode response: %w", err)
	}

	if response.Type == "ERROR" {
		return cluster.GroupState{}, fmt.Errorf("group %s not found", group)
	}

	if response.Type != "GROUP_STATE" {
		return cluster.GroupState{}, fmt.Errorf("unexpected response type: %s", response.Type)
	}

	return response.Group, nil
}
