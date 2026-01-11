package cluster

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	raftlogger "gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/raft_logger"
)

type GroupRaftPool struct {
	mu    sync.RWMutex
	Nodes map[string]*raft.Raft `json:"nodes,omitempty"`
	Fsms  map[string]*FSM       `json:"fsms,omitempty"`
	log   *zerolog.Logger
}

type GroupConfig struct {
	ElectionTimeout time.Duration `json:"electionTimeout,omitempty"`
}

func NewGroupRaftPool(log *zerolog.Logger) *GroupRaftPool {
	return &GroupRaftPool{
		Nodes: make(map[string]*raft.Raft),
		Fsms:  make(map[string]*FSM),
		log:   log,
	}
}

func (p *GroupRaftPool) CreateGroup(group string, cfg GroupConfig) (*raft.Raft, *FSM, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.Nodes[group]; exists {
		return nil, nil, fmt.Errorf("group already exists")
	}
	node, fsm := CreateRaft(group, cfg, p.log)

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(group + "-node1")

	p.Nodes[group] = node
	p.Fsms[group] = fsm
	return node, fsm, nil
}

func CreateRaft(group string, cfg GroupConfig, log *zerolog.Logger) (*raft.Raft, *FSM) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(group)

	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	groupLog := log.With().Str("group", group).Logger()
	raftConfig.Logger = raftlogger.NewRaftZerologAdapter(&groupLog)
	/*/
	dataDir := "./data-dir-raft/" + group + "/"
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		log.Panic().Err(err).Msg(err.Error())
	}
	/*/
	raftPort := 0

	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", raftPort))
	if err != nil {
		log.Panic().Err(err).Msg(err.Error())
	}

	logStore, snapshotStore, stableStore, raftTransport, err := setupRaft(addr)
	if err != nil {
		log.Panic().Err(err).Msg(err.Error())
	}

	fsm := NewFSM(cfg)

	raftNode, err := raft.NewRaft(
		raftConfig,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		raftTransport,
	)
	if err != nil {
		log.Panic().Err(err).Msg(err.Error())
	}

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raftConfig.LocalID,
				Address: raftTransport.LocalAddr(),
			},
		},
	}
	raftNode.BootstrapCluster(configuration)

	time.Sleep(5 * time.Second)
	return raftNode, fsm
}

func setupRaft(addr *net.TCPAddr) (
	raft.LogStore,
	raft.SnapshotStore,
	raft.StableStore,
	raft.Transport,
	error,
) {
	transport, err := raft.NewTCPTransport(addr.String(), addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	store := raft.NewInmemStore()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	stableStore := raft.NewInmemStore()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	snapshots := raft.NewInmemSnapshotStore()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return store, snapshots, stableStore, transport, err
}
