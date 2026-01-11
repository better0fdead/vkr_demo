package server

import (
	"encoding/json"

	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/internal/cluster"
)

type Message struct {
	Op     string          `json:"op"`
	Group  string          `json:"group"`
	Member string          `json:"member"`
	Config json.RawMessage `json:"config,omitempty"`
}

type Response struct {
	Type     string `json:"type"`
	Leader   string `json:"leader"`
	IsLeader bool   `json:"isLeader"`
}

type ResponseState struct {
	Type  string             `json:"type"`
	Group cluster.GroupState `json:"group"`
}

type RedirectResponse struct {
	Type       string `json:"type"`
	LeaderAddr string `json:"leader_addr"`
}
