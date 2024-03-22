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

	lockId := "lock2024"
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

	lockId := "kalock2024"
	locked, ch, err := lock.KALock(context.TODO(), lockId, 3*time.Second)
	if err != nil {
		panic(err)
	}

	if locked {
		defer func() {
			if err := lock.Unlock(context.TODO(), lockId); err != nil {
				log.Fatal(err)
			}
		}()
		fmt.Printf("%s acquired\n", lockId)
		select {
		case karesp := <-ch:
			fmt.Printf("remain ttl: %d, err: %v\n", karesp.TTL, karesp.Err)
		case <-time.After(10 * time.Second):
		}
	}
}

func main() {
	//lock()
	kalock()
}
