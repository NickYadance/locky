package locky

import (
	"context"
	"time"
)

type DistributedLock interface {
	Lock(lockId string, ttl time.Duration) (bool, error)
	LockContext(ctx context.Context, lockId string, ttl time.Duration) (bool, error)
	Unlock(lockId string) error
	UnlockContext(ctx context.Context, lockId string) error
}
