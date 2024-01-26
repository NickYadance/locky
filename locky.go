package locky

import (
	"time"
)

type DistributedLock interface {
	Lock(lockId string, ttl time.Duration) (bool, error)
	Unlock(lockId string) error
}
