package servicecluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	raftmembership "github.com/CanonicalLtd/raft-membership"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/cluster"
	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/metrics"
	raftlogger "gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/raft_logger"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type Store struct {
	RaftDir  string
	RaftBind string
	inmem    bool

	mu           sync.Mutex
	groupManager *cluster.GroupRaftPool
	metrics      *metrics.Metrics

	raft *raft.Raft
}

func New(inmem bool, groupManager *cluster.GroupRaftPool, metrics *metrics.Metrics) *Store {
	return &Store{
		groupManager: groupManager,
		inmem:        inmem,
		metrics:      metrics,
	}
}

func (s *Store) Open(localID string, transport raft.Transport, log *zerolog.Logger) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)
	config.ElectionTimeout = 500 * time.Millisecond
	config.HeartbeatTimeout = 250 * time.Millisecond
	config.LeaderLeaseTimeout = 200 * time.Millisecond
	config.SnapshotInterval = 5 * time.Second

	config.Logger = raftlogger.NewRaftZerologAdapter(log)

	var logStore raft.LogStore
	var stableStore raft.StableStore
	var snapshots raft.SnapshotStore
	if s.inmem {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
		snapshots = raft.NewInmemSnapshotStore()
	} else {
		boltDB, err := raftboltdb.New(raftboltdb.Options{
			Path: filepath.Join(s.RaftDir, "raft.db"),
		})
		if err != nil {
			return fmt.Errorf("new bbolt store: %s", err)
		}
		snapshots, err = raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
		if err != nil {
			return fmt.Errorf("file snapshot store: %s", err)
		}
		logStore = boltDB
		stableStore = boltDB
	}

	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}
	ra.BootstrapCluster(configuration)

	if err := s.WaitForBootstrap(30 * time.Second); err != nil {
		return fmt.Errorf("waiting for bootstrap: %s", err)
	}

	return nil
}

func (s *Store) WaitForBootstrap(timeout time.Duration) error {
	log.Info().Msg("Waiting for Raft cluster to bootstrap...")

	start := time.Now()
	for {
		if time.Since(start) > timeout {
			return fmt.Errorf("timeout waiting for cluster to bootstrap")
		}

		leaderAddr := s.raft.Leader()
		if leaderAddr != "" {
			log.Info().Str("leader", string(leaderAddr)).Msg("Raft cluster bootstrapped successfully")
			return nil
		}

		if s.raft.State() == raft.Leader {
			log.Debug().Msg("This node is the Raft leader")
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (s *Store) Join(nodeID, addr string) error {
	log.Warn().Msgf("received join request for remote node %s at %s", nodeID, addr)

	if s.raft == nil {
		log.Warn().Msgf("raft not initialized yet, ignoring join request for %s at %s", nodeID, addr)
		return fmt.Errorf("raft not initialized yet")
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Warn().Msgf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				log.Warn().Msgf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	log.Warn().Msgf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (s *Store) Apply(c Command) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) State(group string) (cluster.GroupState, error) {
	fsm, exists := s.groupManager.Fsms[group]
	var state cluster.GroupState

	if !exists || fsm.Group.Members == nil {
		return state, fmt.Errorf("group %s not found", group)
	}

	fsm.Mu.RLock()
	defer fsm.Mu.RUnlock()
	return fsm.Group, nil
}
func (s *Store) RemoveNode(nodeID string) error {
	if s.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	future := s.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	return future.Error()
}

func (s *Store) IsLeader() bool {
	if s.raft == nil {
		return false
	}
	return s.raft.State() == raft.Leader
}

func (s *Store) GetLeaderInfo() (string, string) {
	if s.raft == nil {
		return "", ""
	}

	leaderAddr := string(s.raft.Leader())
	if leaderAddr == "" {
		return "", ""
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Warn().Msgf("failed to get raft configuration: %v", err)
		return "", leaderAddr
	}

	for _, srv := range configFuture.Configuration().Servers {
		if string(srv.Address) == leaderAddr {
			return string(srv.ID), leaderAddr
		}
	}

	return "", leaderAddr
}

func (s *Store) HandleChangeRequests(requests <-chan *raftmembership.ChangeRequest) {
	for request := range requests {
		var err error

		switch request.Kind() {
		case raftmembership.JoinRequest:
			err = s.Join(string(request.ID()), string(request.Address()))
		case raftmembership.LeaveRequest:
			err = s.RemoveNode(string(request.ID()))
		}

		request.Done(err)
	}
}
