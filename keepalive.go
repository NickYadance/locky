package locky

import (
	"context"
	"time"
)

// KeepAlive the keepalive implementation is heavily inspired from
// github.com/coreos/etcd@v3.3.13/clientv3/lease.go
type KeepAlive struct {
	Chs           []chan<- *KeepAliveResponse
	Ctxs          []context.Context
	NextKeepAlive time.Time
	Donec         chan struct{}
}

type KeepAliveResponse struct {
	LockId string
	TTL    time.Duration
}
