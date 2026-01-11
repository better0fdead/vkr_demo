package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
)

type GroupState struct {
	Leader  string               `json:"leader,omitempty"`
	Members map[string]time.Time `json:"members,omitempty"`
}

type FSM struct {
	Mu     sync.RWMutex `json:"mu,omitempty"`
	Group  GroupState   `json:"group,omitempty"`
	config GroupConfig
}

func NewFSM(config GroupConfig) *FSM {
	return &FSM{
		Group:  GroupState{Leader: "", Members: make(map[string]time.Time)},
		config: config,
	}
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	var entry struct {
		Op     string `json:"op,omitempty"`
		Group  string `json:"group,omitempty"`
		Member string `json:"member,omitempty"`
	}

	if err := json.Unmarshal(l.Data, &entry); err != nil {
		log.Printf("Failed to unmarshal log entry: %v", err)
		return err
	}

	f.Mu.Lock()
	defer f.Mu.Unlock()

	switch entry.Op {
	case "join":
		f.Group.Members[entry.Member] = time.Now()
		if err := f.electLeaderIfNeeded(entry.Group); err != nil {
			log.Printf("Leader election failed: %v", err)
		}

	case "leave":
		delete(f.Group.Members, entry.Member)
		if f.Group.Leader == entry.Member {
			if err := f.electNewLeader(entry.Group); err != nil {
				log.Printf("New leader election failed: %v", err)
			}
		}

	case "hb":
		if err := f.UpdateMemberLastSeen(entry.Group, entry.Member); err != nil {
			log.Printf("Update member last seen failes: %v", err)
		}

	default:
		return fmt.Errorf("unknown operation: %s", entry.Op)
	}

	return nil
}

func (f *FSM) electNewLeader(group string) error {

	if len(f.Group.Members) == 0 {
		return fmt.Errorf("no members left in group %s", group)
	}

	var newLeader string
	var latest time.Time

	for member, lastSeen := range f.Group.Members {
		if lastSeen.After(latest) {
			latest = lastSeen
			newLeader = member
		}
	}

	if newLeader == "" {
		return fmt.Errorf("failed to elect new leader for group %s", group)
	}

	f.Group.Leader = newLeader
	return nil
}

func (f *FSM) UpdateMemberLastSeen(group, member string) error {
	f.Group.Members[member] = time.Now()
	return f.electLeaderIfNeeded(group)
}

func (f *FSM) electLeaderIfNeeded(group string) error {
	if f.Group.Leader == "" || !f.isLeaderActive() {
		return f.electNewLeader(group)
	}
	return nil
}

func (f *FSM) isLeaderActive() bool {
	leader := f.Group.Leader
	lastSeen, exists := f.Group.Members[leader]

	return exists && time.Since(lastSeen) < f.config.ElectionTimeout
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.Mu.RLock()
	defer f.Mu.RUnlock()

	snapshot := f.Group

	return &fsmSnapshot{snapshot: snapshot}, nil
}

type fsmSnapshot struct {
	snapshot GroupState
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(f.snapshot)
	if err != nil {
		err = sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}

func (f *FSM) Restore(snap io.ReadCloser) error {
	var snapshot GroupState
	if err := json.NewDecoder(snap).Decode(&snapshot); err != nil {
		return err
	}

	f.Mu.Lock()
	defer f.Mu.Unlock()
	f.Group = snapshot
	return nil
}
