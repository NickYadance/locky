package mysql

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"sync"
	"testing"
	"time"
)

var db *sql.DB

type Lock struct {
	LockId        string
	LockOwner     string
	LockTimestamp int64
	LockTTL       int64
}

func init() {
	d, err := sql.Open("mysql", "root:root@/test")
	if err != nil {
		panic(err)
	}

	db = d
}

func Test_NewMysqlDistributedLock(t *testing.T) {
	dropTables()
	type args struct {
		db         *sql.DB
		table      string
		ctx        context.Context
		autoCreate bool
		owner      string
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "empty options", args: args{}, wantErr: true},
		{name: "autoCreate", args: args{db: db, autoCreate: true}, wantErr: false},
		{name: "custom table", args: args{db: db, table: "custom_lock_table", ctx: context.Background(), autoCreate: true}, wantErr: false},
		{name: "custom owner", args: args{db: db, table: "custom_lock_table", ctx: context.Background(), autoCreate: false, owner: "custom_owner"}, wantErr: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewMysqlDistributedLock(Opt{
				Db:         test.args.db,
				Table:      test.args.table,
				Ctx:        test.args.ctx,
				AutoCreate: test.args.autoCreate,
				Owner:      test.args.owner,
			})

			if (err != nil) != test.wantErr {
				t.Errorf("NewMysqlDistributedLock error: %v, wantErr: %v", err, test.wantErr)
				return
			}
		})
	}
}

func Test_Lock(t *testing.T) {
	dropTables()
	lock, _ := NewMysqlDistributedLock(Opt{
		Db:         db,
		AutoCreate: true,
	})

	type args struct {
		lockId string
		ttl    time.Duration
	}

	tests := []struct {
		name    string
		args    args
		lockCnt int
		routine int
		wantErr bool
	}{
		{name: "empty name and duration", args: args{lockId: "", ttl: 0}, lockCnt: 0, routine: 1, wantErr: true},
		{name: "empty duration", args: args{lockId: "test-lock", ttl: 0}, lockCnt: 0, routine: 1, wantErr: true},
		{name: "single routine", args: args{lockId: "test-lock0", ttl: time.Second * 3}, lockCnt: 1, routine: 1, wantErr: false},
		{name: "multi routine(100)", args: args{lockId: "test-lock1", ttl: time.Second * 3}, lockCnt: 1, routine: 100, wantErr: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cntC := make(chan int)
			cnt := 0
			ctx := context.TODO()
			var wg sync.WaitGroup
			for i := 0; i < test.routine; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					locked, err := lock.Lock(ctx, test.args.lockId, test.args.ttl)
					if (err != nil) != test.wantErr {
						t.Errorf("Lock error: %v, wantErr: %v", err, test.wantErr)
						return
					}

					if err != nil || !locked {
						return
					}

					cntC <- 1
				}()
			}

			go func() {
				wg.Wait()
				close(cntC)
			}()

			for c := range cntC {
				cnt = cnt + c
			}

			if cnt != test.lockCnt {
				t.Errorf("Multiple/No routine acquired the lock, expected %d, actual %d", test.lockCnt, cnt)
				return
			}

			if test.lockCnt == 0 {
				return
			}

			locked, err := lock.Lock(ctx, test.args.lockId, test.args.ttl)
			if err != nil || locked {
				t.Errorf("The lock should be owned by others")
				return
			}

			time.Sleep(test.args.ttl + 1*time.Second)

			lockRowsInDb, err := getLocksFromDB(lock)
			if err != nil {
				t.Errorf("Failed to read from db: %v", err)
				return
			}

			if len(lockRowsInDb) != test.lockCnt {
				t.Errorf("Unexpected lock rows in database, expected: %d, actual: %d", test.lockCnt, len(lockRowsInDb))
				return
			}

			if test.lockCnt == 1 {
				lockRow := lockRowsInDb[0]
				if lockRow.LockId != test.args.lockId {
					t.Errorf("Unexpected lock name, expected: %s, actual: %s", lockRow.LockId, test.args.lockId)
					return
				}

				if lockRow.LockOwner != lock.Owner {
					t.Errorf("Unexpected lock owner, expected: %s, actual: %s", lockRow.LockOwner, lock.Owner)
					return
				}
			}

			locked, err = lock.Lock(ctx, test.args.lockId, test.args.ttl)
			if err != nil || !locked {
				t.Errorf("The lock should have been outdated")
				return
			}

			if err := lock.Unlock(ctx, test.args.lockId); err != nil {
				t.Errorf("Failed to unlock: %v", err)
				return
			}
		})
	}
}

func dropTables() {
	_, err := db.Exec(fmt.Sprintf("drop table if exists %s", DefaultTable))
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(fmt.Sprintf("drop table if exists %s", "custom_lock_table"))
	if err != nil {
		log.Fatal(err)
	}
}

func getLocksFromDB(lock *DistributedLock) ([]Lock, error) {
	rows, _ := db.Query(fmt.Sprintf("select lock_id, lock_owner, lock_timestamp, lock_ttl from %s", lock.Table))
	var locks []Lock
	for rows.Next() {
		var lock Lock
		err := rows.Scan(&lock.LockId, &lock.LockOwner, &lock.LockTimestamp, &lock.LockTTL)
		if err != nil {
			return nil, err
		}
		locks = append(locks, lock)
	}
	return locks, nil
}
