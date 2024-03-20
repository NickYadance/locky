package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/nickyadance/locky"
	"sync"
	"time"
)

type DistributedLock struct {
	*Opt
	lockStat, unlockStat *sql.Stmt
	keepAlives           map[string]*locky.KeepAlive
	donec                chan struct{}
	ctx                  context.Context
	cancel               context.CancelFunc
	mu                   sync.Mutex
	once                 sync.Once
}

var _ locky.DistributedLock = &DistributedLock{}

const (
	DefaultTable = "locky_mysql_distributed_lock"
	CreateDDL    = "CREATE TABLE if not exists %s" +
		"( " +
		"    `lock_id`     VARCHAR(255)    NOT NULL, " +
		"    `lock_owner`     VARCHAR(255)    NOT NULL, " +
		"    `lock_timestamp` BIGINT UNSIGNED NOT NULL, " +
		"    `lock_ttl`       INT UNSIGNED    NOT NULL, " +
		"    PRIMARY KEY (`lock_id`) " +
		") ENGINE = InnoDB " +
		"  DEFAULT CHARSET = utf8;"
	QueryLock = "INSERT INTO %s (`lock_id`, `lock_owner`, `lock_timestamp`, `lock_ttl`)" +
		"VALUES (?, ?, UNIX_TIMESTAMP(), ?)" +
		"ON DUPLICATE KEY UPDATE" +
		"`lock_timestamp` = IF(UNIX_TIMESTAMP() - `lock_timestamp` > `lock_ttl`, VALUES(`lock_timestamp`), `lock_timestamp`)," +
		"`lock_ttl` = IF(UNIX_TIMESTAMP() - `lock_timestamp` > `lock_ttl`, VALUES(`lock_ttl`), `lock_ttl`);"
	QueryUnlock                  = "DELETE FROM %s WHERE `lock_id` = ? and `lock_owner` = ?;"
	KeepAliveResponseChannelSize = 16
)

func NewMysqlDistributedLock(opt Opt) (*DistributedLock, error) {
	opt.Complete()

	if err := opt.Validate(); err != nil {
		return nil, err
	}

	db := opt.Db
	table := opt.Table
	ctx := opt.Ctx
	if opt.AutoCreate {
		if err := autoCreate(ctx, db, table); err != nil {
			return nil, err
		}
	}

	lockStat, err := db.PrepareContext(ctx, fmt.Sprintf(QueryLock, table))
	if err != nil {
		return nil, err
	}

	unlockStat, err := db.PrepareContext(ctx, fmt.Sprintf(QueryUnlock, table))
	if err != nil {
		return nil, err
	}

	stopCtx, stopCancel := context.WithCancel(ctx)

	return &DistributedLock{
		Opt:        &opt,
		lockStat:   lockStat,
		unlockStat: unlockStat,
		keepAlives: make(map[string]*locky.KeepAlive),
		donec:      make(chan struct{}),
		ctx:        stopCtx,
		cancel:     stopCancel,
	}, nil
}

func (l *DistributedLock) Lock(ctx context.Context, lockId string, ttl time.Duration) (bool, error) {
	if err := l.validateLockId(lockId); err != nil {
		return false, err
	}

	if err := l.validateTTL(ttl); err != nil {
		return false, err
	}

	res, err := l.lockStat.ExecContext(ctx, lockId, l.Owner, ttl.Seconds())
	if err != nil {
		return false, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	if rowsAffected > 0 {
		return true, nil
	} else {
		return false, nil
	}
}

func (l *DistributedLock) Unlock(ctx context.Context, lockId string) error {
	if err := l.validateLockId(lockId); err != nil {
		return err
	}

	_, err := l.unlockStat.ExecContext(ctx, lockId, l.Owner)
	return err
}

func (l *DistributedLock) KALock(ctx context.Context, lockId string, ttl time.Duration) (bool, <-chan *locky.KeepAliveResponse, error) {
	locked, err := l.Lock(ctx, lockId, ttl)
	if err != nil || !locked {
		return false, nil, err
	}

	// keepalive the lock after required
	ch := make(chan *locky.KeepAliveResponse, KeepAliveResponseChannelSize)
	l.mu.Lock()
	ka, ok := l.keepAlives[lockId]
	if !ok {
		l.keepAlives[lockId] = &locky.KeepAlive{
			Chs:           make([]chan<- *locky.KeepAliveResponse, 0),
			Ctxs:          make([]context.Context, 0),
			NextKeepAlive: time.Now(),
			Donec:         make(chan struct{}),
		}
	} else {
		ka.Chs = append(ka.Chs, ch)
		ka.Ctxs = append(ka.Ctxs, ctx)
	}
	l.mu.Unlock()

	l.once.Do(func() {
		l.keepAliveLoop(ka)
	})

	return true, ch, nil
}

func (l *DistributedLock) Close() error {
	close(l.donec)
	<-l.donec
	return nil
}

func (l *DistributedLock) validateLockId(lockId string) error {
	if len(lockId) <= 0 || len(lockId) > 255 {
		return errors.New("lockId len must be between 0-255")
	}

	return nil
}

func (l *DistributedLock) validateTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("ttl must be non-zero value")
	}

	return nil
}

func (l *DistributedLock) keepAliveLoop(ka *locky.KeepAlive) {

}

func autoCreate(ctx context.Context, db *sql.DB, table string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(CreateDDL, table))
	return err
}
