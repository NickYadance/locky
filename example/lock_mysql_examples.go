package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/nickyadance/locky/mysql"
	"log"
	"time"
)

var db *sql.DB

func init() {
	var err error
	db, err = sql.Open("mysql", "root:root@/test")
	if err != nil {
		panic(err)
	}
}

func lock() {
	lock, err := mysql.NewMysqlDistributedLock(mysql.Opt{
		Db: db,
	})
	if err != nil {
		log.Fatal(err)
	}

	lockId := "lock"
	locked, err := lock.Lock(context.TODO(), lockId, 3*time.Second)
	if err != nil {
		panic(err)
	}

	if locked {
		defer func() {
			if err := lock.Unlock(context.TODO(), lockId); err != nil {
				log.Fatal(err)
			}
		}()
		fmt.Printf("%s acquired", lockId)
	}
}

func kalock() {
	lock, err := mysql.NewMysqlDistributedLock(mysql.Opt{
		Db: db,
	})
	if err != nil {
		log.Fatal(err)
	}

	lockId := "kalock"
	locked, ch, err := lock.KALock(context.TODO(), lockId, 6*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	if locked {
		defer func() {
			if err := lock.Unlock(context.TODO(), lockId); err != nil {
				log.Fatal(err)
			}
		}()
		fmt.Printf("%s acquired\n", lockId)
		for {
			select {
			case karesp, ok := <-ch:
				if !ok {
					return
				}
				fmt.Printf("ttl: %dms, err: %v\n", karesp.TTL.Milliseconds(), karesp.Err)
			}
		}
	}
}

func main() {
	//lock()
	kalock()
}
