package config

import "github.com/kelseyhightower/envconfig"

type RaftConfig struct {
	DataDir string `envconfig:"RAFT_DIR" default:"data-dir"`
	NodeId  string `envconfig:"NODE_ID" default:""`
	Seed    string `envconfig:"SEED" default:""`
	Bind    string `envconfig:"BIND_RAFT" default:":9100"`
}

var raftConfig *RaftConfig

func Raft() RaftConfig {

	if raftConfig != nil {
		return *raftConfig
	}
	raftConfig = &RaftConfig{}
	if err := envconfig.Process("", raftConfig); err != nil {
		panic(err)
	}
	return *raftConfig
}
