package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type MysqlDistributedLock struct {
	*Opt
	lockStat, unlockStat *sql.Stmt
}

const (
	DefaultTable = "locky_mysql_distributed_lock"
	CreateDDL    = "CREATE TABLE if not exists %s" +
		"( " +
		"    `lock_name`     VARCHAR(255)    NOT NULL, " +
		"    `lock_owner`     VARCHAR(255)    NOT NULL, " +
		"    `lock_timestamp` BIGINT UNSIGNED NOT NULL, " +
		"    `lock_ttl`       INT UNSIGNED    NOT NULL, " +
		"    PRIMARY KEY (`lock_name`) " +
		") ENGINE = InnoDB " +
		"  DEFAULT CHARSET = utf8;"
	QueryLock = "INSERT INTO %s (`lock_name`, `lock_owner`, `lock_timestamp`, `lock_ttl`)" +
		"VALUES (?, ?, UNIX_TIMESTAMP(), ?)" +
		"ON DUPLICATE KEY UPDATE" +
		"`lock_timestamp` = IF(UNIX_TIMESTAMP() - `lock_timestamp` > `lock_ttl`, VALUES(`lock_timestamp`), `lock_timestamp`)," +
		"`lock_ttl` = IF(UNIX_TIMESTAMP() - `lock_timestamp` > `lock_ttl`, VALUES(`lock_ttl`), `lock_ttl`);"
	QueryUnlock = "DELETE FROM %s WHERE `lock_name` = ? and `lock_owner` = ?;"
)

func NewMysqlDistributedLock(opt Opt) (*MysqlDistributedLock, error) {
	if err := opt.DefaultAndValidate(); err != nil {
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

	return &MysqlDistributedLock{
		Opt:        &opt,
		lockStat:   lockStat,
		unlockStat: unlockStat,
	}, nil
}

func (l *MysqlDistributedLock) Lock(name string, ttl time.Duration) (bool, error) {
	if err := l.validateName(name); err != nil {
		return false, err
	}

	if err := l.validateTTL(ttl); err != nil {
		return false, err
	}

	res, err := l.lockStat.Exec(name, l.Owner, ttl.Seconds())
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

func (l *MysqlDistributedLock) Unlock(name string) error {
	if err := l.validateName(name); err != nil {
		return err
	}

	_, err := l.unlockStat.Exec(name, l.Owner)
	return err
}

func (l *MysqlDistributedLock) validateName(name string) error {
	if len(name) <= 0 || len(name) > 255 {
		return errors.New("name len must be between 0-255")
	}

	return nil
}

func (l *MysqlDistributedLock) validateTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("ttl must be non-zero value")
	}

	return nil
}

func autoCreate(ctx context.Context, db *sql.DB, table string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(CreateDDL, table))
	return err
}
