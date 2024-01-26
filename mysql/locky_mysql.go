package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type DistributedLock struct {
	*Opt
	lockStat, unlockStat *sql.Stmt
}

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
	QueryUnlock = "DELETE FROM %s WHERE `lock_id` = ? and `lock_owner` = ?;"
)

func NewMysqlDistributedLock(opt Opt) (*DistributedLock, error) {
	opt.Complete()

	if err := opt.Validate(); err != nil {
		return nil, err
	}

	db := opt.Db
	table := opt.Table
	if opt.AutoCreate {
		if err := autoCreate(db, table); err != nil {
			return nil, err
		}
	}

	lockStat, err := db.Prepare(fmt.Sprintf(QueryLock, table))
	if err != nil {
		return nil, err
	}

	unlockStat, err := db.Prepare(fmt.Sprintf(QueryUnlock, table))
	if err != nil {
		return nil, err
	}

	return &DistributedLock{
		Opt:        &opt,
		lockStat:   lockStat,
		unlockStat: unlockStat,
	}, nil
}

func (l *DistributedLock) Lock(lockId string, ttl time.Duration) (bool, error) {
	return l.LockContext(context.TODO(), lockId, ttl)
}

func (l *DistributedLock) LockContext(ctx context.Context, lockId string, ttl time.Duration) (bool, error) {
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

func (l *DistributedLock) Unlock(lockId string) error {
	return l.UnlockContext(context.TODO(), lockId)
}

func (l *DistributedLock) UnlockContext(ctx context.Context, lockId string) error {
	if err := l.validateLockId(lockId); err != nil {
		return err
	}

	_, err := l.unlockStat.ExecContext(ctx, lockId, l.Owner)
	return err
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

func autoCreate(db *sql.DB, table string) error {
	_, err := db.Exec(fmt.Sprintf(CreateDDL, table))
	return err
}
