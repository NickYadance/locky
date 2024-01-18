package locky

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type Opt struct {
	Db         *sql.DB
	Table      string
	Ctx        context.Context
	AutoCreate bool
}

func (o *Opt) Default() {
	if len(o.Table) == 0 {
		o.Table = DefaultTable
	}

	if o.Ctx == nil {
		o.Ctx = context.Background()
	}
}

func (o *Opt) Validate() error {
	if o.Db == nil {
		return errors.New("invalid db")
	}

	if len(o.Table) > 255 {
		return errors.New("table name too long(>255)")
	}

	if err := o.Db.PingContext(o.Ctx); err != nil {
		return err
	}

	return nil
}

type MysqlDistributedLock struct {
	db                   *sql.DB
	table                string
	ctx                  context.Context
	lockStat, unlockStat *sql.Stmt
}

const (
	DefaultTable = "locky_mysql_distributed_lock"
	CreateDDL    = "CREATE TABLE if not exists %s" +
		"( " +
		"    `lock_name`     VARCHAR(255)    NOT NULL, " +
		"    `lock_timestamp` BIGINT UNSIGNED NOT NULL, " +
		"    `lock_ttl`       INT UNSIGNED    NOT NULL, " +
		"    PRIMARY KEY (`lock_name`) " +
		") ENGINE = InnoDB " +
		"  DEFAULT CHARSET = utf8;"
	QueryLock = "INSERT INTO %s (`lock_name`, `lock_timestamp`, `lock_ttl`)" +
		"VALUES (?, UNIX_TIMESTAMP(), ?)" +
		"ON DUPLICATE KEY UPDATE" +
		"`lock_timestamp` = IF(UNIX_TIMESTAMP() - `lock_timestamp` > `lock_ttl`, VALUES(`lock_timestamp`), `lock_timestamp`)," +
		"`lock_ttl` = IF(UNIX_TIMESTAMP() - `lock_timestamp` > `lock_ttl`, VALUES(`lock_ttl`), `lock_ttl`);"
	QueryUnlock = "DELETE FROM %s WHERE `lock_name` = ?;"
)

func NewMysqlDistributedLock(opt Opt) (DistributedLock, error) {
	opt.Default()
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

	return &MysqlDistributedLock{
		db:         db,
		table:      table,
		ctx:        ctx,
		lockStat:   lockStat,
		unlockStat: unlockStat,
	}, nil
}

func autoCreate(ctx context.Context, db *sql.DB, table string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(CreateDDL, table))
	return err
}

func (l *MysqlDistributedLock) Lock(owner string, ttl time.Duration) (bool, error) {
	if err := l.validateOwner(owner); err != nil {
		return false, err
	}

	if err := l.validateTTL(ttl); err != nil {
		return false, err
	}

	res, err := l.lockStat.Exec(owner, ttl.Seconds())
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

func (l *MysqlDistributedLock) Unlock(owner string) error {
	if err := l.validateOwner(owner); err != nil {
		return err
	}

	_, err := l.unlockStat.Exec(owner)
	return err
}

func (l *MysqlDistributedLock) validateOwner(owner string) error {
	if len(owner) <= 0 || len(owner) > 255 {
		return errors.New("owner len must be between 0-255")
	}

	return nil
}

func (l *MysqlDistributedLock) validateTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("ttl must be non-zero value")
	}

	return nil
}
