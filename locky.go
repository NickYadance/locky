package locky

import (
	"time"
)

type DistributedLock interface {
	Lock(owner string, ttl time.Duration) (bool, error)
	Unlock(owner string) error
}
