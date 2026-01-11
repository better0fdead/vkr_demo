package rafttransport

import (
	"net"
	"time"
)

type Dial func(addr string, timeout time.Duration) (net.Conn, error)

func NewDial() Dial {
	dial := func(addr string, timeout time.Duration) (net.Conn, error) {
		dialer := &net.Dialer{Timeout: timeout}
		return dialer.Dial("tcp", addr)
	}
	return dial
}
