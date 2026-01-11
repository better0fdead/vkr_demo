package servicecluster

import (
	"encoding/json"
)

type Message struct {
	Op     string          `json:"op"`
	Group  string          `json:"group"`
	Member string          `json:"member"`
	Config json.RawMessage `json:"config,omitempty"`
}

type Command struct {
	Message Message `json:"message,omitempty"`
}
