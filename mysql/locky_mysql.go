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
	lockStat, unlockStat, keepAliveStat, ttlStat *sql.Stmt
	keepAlives                                   map[string]*locky.KeepAlive
	donec                                        chan struct{}
	ctx                                          context.Context
	cancel                                       context.CancelFunc
	mu                                           sync.Mutex
	once                                         sync.Once
}

var _ locky.DistributedLock = &DistributedLock{}

const (
	DefaultTable = "locky_mysql_distributed_lock"
	DDL          = "CREATE TABLE if not exists %s" +
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
	QueryUnlock    = "DELETE FROM %s WHERE `lock_id` = ? and `lock_owner` = ?;"
	QueryKeepAlive = "update %s set " +
		"`lock_timestamp` = IF(unix_timestamp() - `lock_timestamp` < `lock_ttl`, unix_timestamp(), `lock_timestamp`) " +
		"where lock_id = ? and lock_owner = ?"
	QueryTTL                     = "select UNIX_TIMESTAMP() - `lock_timestamp` as ttl from %s where lock_id = ? and lock_owner = ?"
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

	keepAliveStat, err := db.PrepareContext(ctx, fmt.Sprintf(QueryKeepAlive, table))
	if err != nil {
		return nil, err
	}

	ttlStat, err := db.PrepareContext(ctx, fmt.Sprintf(QueryTTL, table))
	if err != nil {
		return nil, err
	}

	stopCtx, stopCancel := context.WithCancel(ctx)

	return &DistributedLock{
		Opt:           &opt,
		lockStat:      lockStat,
		unlockStat:    unlockStat,
		keepAliveStat: keepAliveStat,
		ttlStat:       ttlStat,
		keepAlives:    make(map[string]*locky.KeepAlive),
		donec:         make(chan struct{}),
		ctx:           stopCtx,
		cancel:        stopCancel,
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

	ch, err := l.keepAlive(ctx, lockId)
	return locked, ch, err
}

func (l *DistributedLock) Close() error {
	close(l.donec)
	l.mu.Lock()
	for _, ka := range l.keepAlives {
		ka.Close()
	}
	l.keepAlives = make(map[string]*locky.KeepAlive)
	l.mu.Unlock()
	<-l.donec
	return nil
}

func (l *DistributedLock) keepAlive(ctx context.Context, lockId string) (<-chan *locky.KeepAliveResponse, error) {
	ch := make(chan *locky.KeepAliveResponse, KeepAliveResponseChannelSize)
	l.mu.Lock()
	ka, ok := l.keepAlives[lockId]
	if !ok {
		l.keepAlives[lockId] = &locky.KeepAlive{
			Ch:            ch,
			Ctx:           ctx,
			NextKeepAlive: time.Now(),
			Donec:         make(chan struct{}),
		}
		l.mu.Unlock()
	} else {
		l.mu.Unlock()
		return ka.Ch, nil
	}

	go l.keepAliveContextCloser(ka)

	l.once.Do(func() {
		l.keepAliveLoop()
		l.deadlineLoop()
	})

	return ch, nil
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

func (l *DistributedLock) keepAliveContextCloser(ka *locky.KeepAlive) {
	select {
	case <-l.donec:
	case <-ka.Donec:
	case <-ka.Ctx.Done():
		ka.Close()
		l.mu.Lock()
		delete(l.keepAlives, ka.LockId)
		l.mu.Unlock()
	}
}

func (l *DistributedLock) keepAliveLoop() {
	for {
		var toSend []*locky.KeepAlive
		l.mu.Lock()
		for _, ka := range l.keepAlives {
			if time.Now().After(ka.NextKeepAlive) {
				toSend = append(toSend, ka)
			}
		}
		l.mu.Unlock()

		for _, ka := range toSend {
			karesp := l.sendKeepAlive(ka)
			select {
			case ka.Ch <- karesp:
			default:
			}

			if errors.Is(karesp.Err, ErrLockExpired) {
				ka.Close()
				l.mu.Lock()
				delete(l.keepAlives, ka.LockId)
				l.mu.Unlock()
			}

			ka.NextKeepAlive = time.Now().Add(karesp.TTL / 3)
		}

		select {
		case <-time.After(500 * time.Millisecond):
		case <-l.donec:
			return
		}
	}

}

func (l *DistributedLock) sendKeepAlive(ka *locky.KeepAlive) *locky.KeepAliveResponse {
	karesp := &locky.KeepAliveResponse{}
	result, err := l.keepAliveStat.ExecContext(ka.Ctx, ka.LockId, l.Owner)
	if err != nil {
		karesp.Err = err
		return karesp
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		karesp.Err = err
		return karesp
	}

	if rowsAffected == 0 {
		karesp.Err = ErrLockExpired
		return karesp
	}

	var ttl int64
	row := l.ttlStat.QueryRowContext(ka.Ctx, ka.LockId, l.Owner)
	if err := row.Scan(&ttl); err != nil {
		karesp.Err = err
		return karesp
	}

	karesp.TTL = time.Duration(ttl) * time.Second
	karesp.Err = nil
	return karesp
}

func (l *DistributedLock) deadlineLoop() {
	for {
		select {
		case <-time.After(time.Second):
		case <-l.donec:
			return
		}
		l.mu.Lock()
		for _, ka := range l.keepAlives {
			if time.Now().After(ka.Deadline) {
				select {
				case ka.Ch <- &locky.KeepAliveResponse{Err: ErrDeadlineReached}:
				default:
				}
				ka.Close()
				delete(l.keepAlives, ka.LockId)
			}
		}
		l.mu.Unlock()
	}
}

func autoCreate(ctx context.Context, db *sql.DB, table string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(DDL, table))
	return err
}
