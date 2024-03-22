package mysql

import "fmt"

var ErrLockExpired = fmt.Errorf("lock expired")
