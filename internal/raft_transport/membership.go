package rafttransport

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	raftmembership "github.com/CanonicalLtd/raft-membership"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func ChangeMembership(
	kind raftmembership.ChangeRequestKind,
	path string,
	dial Dial,
	id raft.ServerID,
	address, target string,
	timeout time.Duration) error {
	url, _ := url.Parse(path)
	url.RawQuery = fmt.Sprintf("id=%s", id)
	if kind == raftmembership.JoinRequest {
		url.RawQuery += fmt.Sprintf("&address=%s", address)
	}
	url.Host = target
	url.Scheme = "http"
	method := membershipChangeRequestKindToMethod[kind]
	request := &http.Request{
		Method:     method,
		URL:        url,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
	}

	remaining := timeout
	var response *http.Response
	var err error
	for remaining > 0 {
		start := time.Now()
		netDial := func(network, addr string) (net.Conn, error) {
			return dial(addr, remaining)
		}
		client := &http.Client{
			Timeout:   remaining,
			Transport: &http.Transport{Dial: netDial},
		}
		response, err = client.Do(request)

		if err != nil {
			break
		}

		if response.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(response.Body)
			err = fmt.Errorf(
				"http code %d '%s'", response.StatusCode,
				strings.TrimSpace(string(body)))
		}

		if response.StatusCode == http.StatusServiceUnavailable {
			time.Sleep(100 * time.Millisecond)

			remaining -= time.Since(start)
			continue
		}

		break
	}
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("server %s failed", kind))
	}
	return nil
}
