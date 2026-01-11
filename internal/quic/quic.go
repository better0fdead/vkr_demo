package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/discovery"
	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/metrics"
	rafttransport "gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/raft_transport"

	servicecluster "gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/service_cluster"

	"github.com/gofiber/fiber/v2"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/logging"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type QuicServer struct {
	server    *http3.Server
	store     *servicecluster.Store
	discovery *discovery.QuicDiscovery
	mux       *http.ServeMux
	logger    *zerolog.Logger
	metrics   *metrics.Metrics
	srvHealth *fiber.App
}

var sv *QuicServer

func NewQuicServer(tlsConf *tls.Config, addr string, store *servicecluster.Store, logger *zerolog.Logger, metrics *metrics.Metrics) *QuicServer {
	mux := http.NewServeMux()

	metricsMiddleware := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, req *http.Request) {
			startTime := time.Now()
			metrics.RequestsInFlight.Inc()
			metrics.RequestsTotal.Inc()

			if req.ContentLength > 0 {
				metrics.RequestSizeBytes.Observe(float64(req.ContentLength))
			}

			next(w, req)

			metrics.RequestsInFlight.Dec()
			metrics.RequestDuration.Observe(time.Since(startTime).Seconds())
		}
	}

	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/stream", metricsMiddleware(handleStream))
	mux.HandleFunc("/discovery", metricsMiddleware(handleDiscovery))

	return &QuicServer{
		server: &http3.Server{
			Handler:   mux,
			TLSConfig: tlsConf,
			Addr:      addr,
			QUICConfig: &quic.Config{
				MaxIdleTimeout: 5 * time.Second,
				Tracer: func(
					ctx context.Context,
					p logging.Perspective,
					connID quic.ConnectionID,
				) *logging.ConnectionTracer {
					tracer := logging.ConnectionTracer{
						StartedConnection: func(local, remote net.Addr, srcConnID, destConnID logging.ConnectionID) {
							metrics.ActiveConnections.Inc()
							metrics.TotalConnections.Inc()
						},
						ClosedConnection: func(err error) {
							if err != nil {
								metrics.ConnectionErrors.Inc()
							}
							metrics.ActiveConnections.Dec()
						}}
					return &tracer
				},
			},
		},
		store:   store,
		mux:     mux,
		logger:  logger,
		metrics: metrics,
	}
}

func handleStream(w http.ResponseWriter, req *http.Request) {
	log.Debug().Msg("Received stream request")

	w.WriteHeader(http.StatusOK)

	stream := w.(http3.HTTPStreamer).HTTPStream()
	if stream == nil {
		sv.logger.Error().Msg("HTTP stream is nil")
		return
	}

	_, err := stream.Write([]byte("connected"))
	if err != nil {
		sv.logger.Error().Msg("Failed to write to stream")
		return
	}

	log.Debug().Msg("Stream from client was accepted")

	go func() {
		defer func() {
			if err := stream.Close(); err != nil {
				sv.logger.Warn().Err(err).Msg("Error closing stream")
			}
		}()

		sv.handleHeartbeats(stream)
	}()
}

func (qs *QuicServer) InitRaftTransport(raftAddr string) (*raft.NetworkTransport, error) {
	listener, err := net.Listen("tcp", raftAddr)
	if err != nil {
		qs.logger.Err(err).Msg("listening to local port failed")
	}

	raftHandler := rafttransport.NewHandler(qs.logger)

	raftLayer := rafttransport.NewLayer("/raft", raftAddr, raftHandler, rafttransport.NewDial(), qs.logger)

	transport := raft.NewNetworkTransport(
		raftLayer,
		20,
		5*time.Second,
		qs.logger,
	)
	server := &http.Server{Handler: raftHandler, ReadHeaderTimeout: time.Second * 5}

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Panic().Err(err).Msg(err.Error())
		}
	}()

	go qs.store.HandleChangeRequests(raftHandler.Requests())

	log.Info().Str("addr", raftAddr).Msg("Initialized Raft transport")
	return transport, nil
}

func handleDiscovery(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if sv.discovery == nil {
		http.Error(w, "Discovery service not initialized", http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		sv.logger.Error().Err(err).Msg("Failed to read discovery message")
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	response, err := sv.discovery.HandleDiscoveryMessage(body)
	if err != nil {
		sv.logger.Error().Err(err).Msg("Failed to process discovery message")
		http.Error(w, "Failed to process message", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(response)
	if err != nil {
		log.Error().Err(err).Msg("Failed to write discovery response")
	}
}

func (qs *QuicServer) Start() error {
	sv = qs
	qs.logger.Info().Msgf("Server is listening on %s", qs.server.Addr)
	return qs.server.ListenAndServe()
}

func (qs *QuicServer) InitDiscovery(nodeID string, raftAddr string, tlsConfig *tls.Config) error {
	disc, err := discovery.NewQuicDiscovery(
		nodeID,
		raftAddr,
		qs.server.Addr,
		tlsConfig,
		qs.handleJoin,
		qs.handleLeave,
		discovery.WithAdvertiseAddress(strings.Split(qs.server.Addr, ":")[0]),
	)

	if err != nil {
		return err
	}

	if err := disc.Start(); err != nil {
		return err
	}

	qs.discovery = disc
	return nil
}

func (qs *QuicServer) JoinCluster(seeds []string) (int, error) {
	if qs.discovery == nil {
		return 0, fmt.Errorf("discovery service not initialized")
	}

	return qs.discovery.Join(seeds)
}

func (qs *QuicServer) handleJoin(nodeID string, raftAddr string) error {
	qs.logger.Info().Str("nodeID", nodeID).Str("raftAddr", raftAddr).Msg("Node joined the cluster")

	return qs.store.Join(nodeID, raftAddr)
}

func (qs *QuicServer) handleLeave(nodeID string) error {
	qs.logger.Info().Str("nodeID", nodeID).Msg("Node left the cluster")

	return qs.store.RemoveNode(nodeID)
}

func (qs *QuicServer) isLeader() bool {
	return qs.store.IsLeader()
}

func (qs *QuicServer) getLeaderAddress() string {
	leaderID, leaderRaftAddr := qs.store.GetLeaderInfo()
	if leaderRaftAddr == "" {
		return ""
	}
	if qs.discovery != nil {
		nodes := qs.discovery.GetNodes()
		for _, node := range nodes {
			if node.ID == leaderID && node.Addr != "" {
				return node.Addr
			}
		}
	}

	return formatLeaderAddr(leaderRaftAddr)
}

func formatLeaderAddr(raftAddr string) string {
	parts := strings.Split(raftAddr, ":")
	if len(parts) >= 1 {
		return parts[0] + ":4242"
	}
	return raftAddr
}

func (qs *QuicServer) handleHeartbeats(stream http3.Stream) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn().Msgf("Message handler panic: %v", r)
		}
	}()

	decoder := json.NewDecoder(stream)
	for {
		var msg servicecluster.Message
		err := decoder.Decode(&msg)
		if err != nil {
			if err != io.EOF {
				log.Warn().Msgf("Decode error: %v", err)
			}
			return
		}

		log.Debug().Msgf("Received message: %+v", msg)

		qs.metrics.GroupOperations.WithLabelValues(msg.Op).Inc()
		startTime := time.Now()

		if !qs.isLeader() {
			leaderAddr := qs.getLeaderAddress()
			if leaderAddr == "" {
				log.Warn().Msg("Not leader and couldn't determine leader address")
				response := Response{
					Type:     "ERROR",
					Leader:   "",
					IsLeader: false,
				}
				if err := json.NewEncoder(stream).Encode(response); err != nil {
					log.Warn().Msgf("write error response failed: %v", err)
				}
				continue
			}

			redirectResp := RedirectResponse{
				Type:       "REDIRECT",
				LeaderAddr: leaderAddr,
			}

			if err := json.NewEncoder(stream).Encode(redirectResp); err != nil {
				log.Warn().Msgf("write redirect response failed: %v", err)
			}
			continue
		}

		m := servicecluster.Command{Message: msg}
		if msg.Op == "GET_STATE" {
			state, err := qs.store.State(msg.Group)
			qs.metrics.GroupLatency.WithLabelValues("GET_STATE").Observe(time.Since(startTime).Seconds())

			if err != nil {
				log.Warn().Msgf("Operation %s failed: %v", msg.Op, err)
			}
			response := ResponseState{
				Type:  "GROUP_STATE",
				Group: state,
			}

			if err := json.NewEncoder(stream).Encode(response); err != nil {
				log.Warn().Msgf("write response failed: %v", err)
			}
			continue
		}
		err = qs.store.Apply(m)
		if err != nil {
			log.Warn().Msgf("Operation %s failed: %v", msg.Op, err)
		}
		state, err := qs.store.State(msg.Group)

		qs.metrics.GroupLatency.WithLabelValues(msg.Op).Observe(time.Since(startTime).Seconds())

		if err != nil {
			log.Warn().Msgf("Operation %s failed: %v", msg.Op, err)
		}

		response, err := json.Marshal(Response{
			Type:     "LEADER",
			Leader:   state.Leader,
			IsLeader: state.Leader == msg.Member,
		})

		if err != nil {
			log.Warn().Msgf("write response failed: %v", err)
		}

		if _, err := stream.Write(response); err != nil {
			log.Warn().Msgf("write response failed: %v", err)
		}

		if err != nil {
			log.Warn().Msgf("Operation %s failed: %v", msg.Op, err)
		}
	}
}

func (srv *QuicServer) ServeHealth(address string, response interface{}) {
	srv.srvHealth = fiber.New(fiber.Config{DisableStartupMessage: true})
	srv.srvHealth.Get("/health", func(ctx *fiber.Ctx) error {
		return ctx.JSON(response)
	})
	go func() {
		err := srv.srvHealth.Listen(address)
		if err != nil {
			srv.logger.Panic().Err(err).Msg("serve health on " + address)
		}
	}()
}
