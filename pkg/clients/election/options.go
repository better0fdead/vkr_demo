package election

import (
	"crypto/tls"
	"time"
)

type Option func(*Client)

func WithMember(member string) Option {
	return func(c *Client) {
		c.member = member
	}
}

func WithRetryInterval(retryInterval time.Duration) Option {
	return func(c *Client) {
		c.retryInterval = retryInterval
	}
}

func WithAddress(addr string) Option {
	return func(c *Client) {
		c.addr = addr + "/stream"
	}
}

func WithElectionTimeout(electionTimeout time.Duration) Option {
	return func(c *Client) {
		c.electionTimeout = electionTimeout
	}
}

func WithTlsConfig(tlsConf *tls.Config) Option {
	return func(c *Client) {
		c.tlsConf = tlsConf
	}
}

func WithCallbacks(callbacks LeaderCallbacks) Option {
	return func(c *Client) {
		c.leaderCallbacks = callbacks
	}
}

func WithHeartbeatRate(heartbeatRate time.Duration) Option {
	return func(c *Client) {
		c.heartbeatRate = heartbeatRate
	}
}
