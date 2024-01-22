package locky

import (
	"time"
)

type DistributedLock interface {
	Lock(name string, ttl time.Duration) (bool, error)
	Unlock(name string) error
}
