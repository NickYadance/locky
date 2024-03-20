package locky

import (
	"context"
	"time"
)

type DistributedLock interface {
	Lock(ctx context.Context, lockId string, ttl time.Duration) (bool, error)
	Unlock(ctx context.Context, lockId string) error
	// KALock Lock with keepalive
	KALock(ctx context.Context, lockId string, ttl time.Duration) (bool, <-chan *KeepAliveResponse, error)
	Close() error
}
