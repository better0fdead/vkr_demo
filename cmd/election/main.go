package main

import (
	"crypto/tls"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	_ "go.uber.org/automaxprocs"

	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/cert"
	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/cluster"
	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/metrics"
	server "gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/quic"
	servicecluster "gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/service_cluster"

	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/config"
)

const (
	serviceName = "election"
)

func main() {
	service := config.Service()
	raftCfg := config.Raft()
	metrics := metrics.NewMetrics("election")
	log.Logger = service.Logger().With().Str("service", serviceName).Logger()
	var nodeID string
	if raftCfg.NodeId == "" {
		nodeID = service.Bind
	} else {
		nodeID = raftCfg.NodeId
	}
	/*/

	raftDir := raftCfg.DataDir
	if raftDir == "" {
		log.Panic().Msgf("No Raft storage directory specified")
	}

	if err := os.MkdirAll(raftDir, 0600); err != nil {
		log.Panic().Msgf("failed to create path for Raft storage: %s", err.Error())
	}

	snapshotsDir := filepath.Join(raftDir, "snapshots")
	if err := os.MkdirAll(snapshotsDir, 0600); err != nil {
		log.Panic().Msgf("failed to create snapshots directory: %s", err.Error())
	}
	s.RaftDir = raftDir
	/*/
	groupManager := cluster.NewGroupRaftPool(&log.Logger)
	s := servicecluster.New(true, groupManager, metrics)

	s.RaftBind = service.Bind

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	log.Info().Str("go", runtime.Version()).Send()

	tlsCert, err := cert.LoadTLSCertificate()
	if err != nil {
		log.Panic().Err(err).Msg("certificate initialization failed")
	}

	caPool, err := cert.LoadCACertPool()
	if err != nil {
		log.Panic().Err(err).Msg("ca certificate pool initialization failed")
	}

	tlsConf := &tls.Config{
		NextProtos:   []string{"h3"},
		Certificates: []tls.Certificate{tlsCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}

	quicServer := server.NewQuicServer(tlsConf, service.Bind, s, &log.Logger, metrics)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.Service().InsecureSkipVerify,
	}

	if err := quicServer.InitDiscovery(nodeID, config.Raft().Bind, tlsConfig); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize discovery")
	}

	raftTrasport, err := quicServer.InitRaftTransport(config.Raft().Bind)
	if err := s.Open(nodeID, raftTrasport, &log.Logger); err != nil {
		log.Fatal().Err(err).Msg("Failed to open store")
	}
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to open store")
	}

	defer log.Info().Msg("goodbye")
	quicServer.ServeHealth(config.Service().HealthBind, "OK")
	go func() {
		if err := quicServer.Start(); err != nil {
			log.Panic().Err(err).Msg(err.Error())
		}
	}()
	time.Sleep(time.Second)
	if raftCfg.Seed != "" {
		seeds := strings.Split(raftCfg.Seed, ",")
		n, err := quicServer.JoinCluster(seeds)
		if err != nil {
			log.Error().Err(err).Msg("Failed to join cluster")
		} else {
			log.Info().Int("nodes", n).Msg("Successfully joined cluster")
		}
	}

	<-shutdown
}
