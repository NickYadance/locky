package mysql

import (
	"database/sql"
	"log"
	"time"
)

func exampleLockAndUnlock() {
	db, err := sql.Open("mysql", "root:root@/test")
	if err != nil {
		panic(err)
	}

	lock, err := NewMysqlDistributedLock(Opt{
		Db:         db,
		Table:      "custom_lock_table",
		AutoCreate: true,
	})

	lockName := "lock2024"
	locked, err := lock.Lock(lockName, 3*time.Second)
	if err != nil {
		panic(err)
	}

	if locked {
		defer func() {
			if err := lock.Unlock(lockName); err != nil {
				log.Println(err)
			}
		}()
		// do your stuff
	}
}
