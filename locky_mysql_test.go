package locky

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"sync"
	"testing"
	"time"
)

var db *sql.DB

func init() {
	d, err := sql.Open("mysql", "root:root@/test")
	if err != nil {
		panic(err)
	}

	db = d
}

func Test_NewMysqlDistributedLock(t *testing.T) {
	db.Exec("drop table if exists custom_lock_table;" +
		"drop table if exists locky_mysql_distributed_lock")

	type args struct {
		db         *sql.DB
		table      string
		ctx        context.Context
		autoCreate bool
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "empty options", args: args{}, wantErr: true},
		{name: "default options(autoCreate)", args: args{db: db, autoCreate: true}, wantErr: false},
		{name: "custom options", args: args{db: db, table: "custom_lock_table", ctx: context.Background(), autoCreate: true}, wantErr: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewMysqlDistributedLock(Opt{
				Db:         test.args.db,
				Table:      test.args.table,
				Ctx:        test.args.ctx,
				AutoCreate: test.args.autoCreate,
			})

			if (err != nil) != test.wantErr {
				t.Errorf("NewMysqlDistributedLock error: %v, wantErr: %v", err, test.wantErr)
				return
			}
		})
	}
}

func Test_Lock(t *testing.T) {
	lock, _ := NewMysqlDistributedLock(Opt{
		Db:         db,
		AutoCreate: true,
	})

	type args struct {
		name string
		ttl  time.Duration
	}

	tests := []struct {
		name    string
		args    args
		lockCnt int
		routine int
		wantErr bool
	}{
		{name: "empty name and duration", args: args{name: "", ttl: 0}, lockCnt: 0, routine: 1, wantErr: true},
		{name: "empty duration", args: args{name: "test-lock", ttl: 0}, lockCnt: 0, routine: 1, wantErr: true},
		{name: "name and duration", args: args{name: "test-lock0", ttl: time.Second * 3}, lockCnt: 1, routine: 1, wantErr: false},
		{name: "multi routine(100)", args: args{name: "test-lock1", ttl: time.Second * 3}, lockCnt: 1, routine: 100, wantErr: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cntC := make(chan int)
			cnt := 0
			var wg sync.WaitGroup
			for i := 0; i < test.routine; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					locked, err := lock.Lock(test.args.name, test.args.ttl)
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

			locked, err := lock.Lock(test.args.name, test.args.ttl)
			if err != nil || locked {
				t.Errorf("The lock should be owned by others")
				return
			}

			time.Sleep(test.args.ttl + 10*time.Millisecond)

			locked, err = lock.Lock(test.args.name, test.args.ttl)
			if err != nil || !locked {
				t.Errorf("The lock should have been outdated")
				return
			}

			if err := lock.Unlock(test.args.name); err != nil {
				t.Errorf("Failed to unlock: %v", err)
				return
			}
		})
	}
}
