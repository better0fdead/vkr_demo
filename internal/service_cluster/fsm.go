package servicecluster

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/cluster"
)

type fsm Store

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	m := c.Message

	var err error

	var raftNode *raft.Raft
	var fsmNode *cluster.FSM
	if _, exists := f.groupManager.Nodes[m.Group]; !exists {
		var cfg cluster.GroupConfig
		if m.Op == "JN" && len(m.Config) > 0 {
			if err := json.Unmarshal(m.Config, &cfg); err != nil {
				log.Warn().Msgf("Failed to parse GroupConfig: %v", err)
			}
		}
		raftNode, fsmNode, _ = f.groupManager.CreateGroup(m.Group, cfg)
		f.metrics.ActiveGroups.Inc()
	} else {
		raftNode = f.groupManager.Nodes[m.Group]
		fsmNode = f.groupManager.Fsms[m.Group]
	}

	switch m.Op {
	case "LV":
		f.metrics.GroupLv.Inc()
		err = f.leave(m.Group, m.Member, raftNode)
	case "JN":
		f.metrics.GroupJn.Inc()
		err = f.join(m.Group, m.Member, raftNode)
	case "HB":
		f.metrics.GroupHb.Inc()
		err = f.processHeartbeat(m.Group, m.Member, raftNode, fsmNode)
	default:
		log.Warn().Msgf("Unknown operation: %s", m.Op)
	}

	if err != nil {
		log.Warn().Msgf("Operation %s failed: %v", m.Op, err)
	}
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	snapshot := &fsmSnapshot{
		Store: cluster.GroupRaftPool{
			Nodes: make(map[string]*raft.Raft),
			Fsms:  make(map[string]*cluster.FSM),
		},
	}

	for groupName, fsm := range f.groupManager.Fsms {
		fsm.Mu.RLock()
		snapshot.Store.Fsms[groupName] = &cluster.FSM{
			Group: fsm.Group,
		}
		fsm.Mu.RUnlock()
	}

	return snapshot, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var snapshot cluster.GroupRaftPool
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return err
	}

	for groupName, fsm := range snapshot.Fsms {
		if existingFsm, ok := f.groupManager.Fsms[groupName]; ok {
			existingFsm.Mu.Lock()
			existingFsm.Group = fsm.Group
			existingFsm.Mu.Unlock()
		} else {
			f.groupManager.Fsms[groupName] = &cluster.FSM{
				Group: fsm.Group,
			}
		}
	}

	return nil
}
func (f *fsm) join(group, member string, raft *raft.Raft) error {
	data, _ := json.Marshal(map[string]string{
		"Op":     "join",
		"Group":  group,
		"Member": member,
	})
	future := raft.Apply(data, 5*time.Second)
	err := future.Error()
	if err != nil {
		return fmt.Errorf("write response failed: %w", err)
	}
	return nil
}

func (f *fsm) leave(group, member string, raft *raft.Raft) error {
	data, _ := json.Marshal(map[string]string{
		"Op":     "leave",
		"Group":  group,
		"Member": member,
	})
	future := raft.Apply(data, 5*time.Second)
	err := future.Error()
	if err != nil {
		return fmt.Errorf("write response failed: %w", err)
	}
	return nil
}

func (f *fsm) processHeartbeat(group, member string, raft *raft.Raft, fsm *cluster.FSM) error {
	cmd := struct {
		Op     string `json:"op"`
		Group  string `json:"group"`
		Member string `json:"member"`
	}{
		Op:     "hb",
		Group:  group,
		Member: member,
	}

	data, _ := json.Marshal(cmd)
	future := raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	fsm.Mu.Lock()
	defer fsm.Mu.Unlock()

	return nil
}

type fsmSnapshot struct {
	Store cluster.GroupRaftPool `json:"store,omitempty"`
}

type serializableStore struct {
	Nodes map[string]*raft.Raft   `json:"nodes,omitempty"`
	Fsms  map[string]*cluster.FSM `json:"fsms,omitempty"`
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		serializableStore := serializableStore{
			Nodes: f.Store.Nodes,
			Fsms:  f.Store.Fsms,
		}

		b, err := json.Marshal(serializableStore)
		if err != nil {
			return err
		}

		if _, err := sink.Write(b); err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		err = sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
