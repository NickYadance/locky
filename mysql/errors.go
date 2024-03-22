package mysql

import "fmt"

var ErrLockExpired = fmt.Errorf("lock expired")
var ErrDeadlineReached = fmt.Errorf("keepalive deadline reached")
