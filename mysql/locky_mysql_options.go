package mysql

import (
	"context"
	"database/sql"
	"errors"
	"github.com/google/uuid"
	"strings"
)

type Opt struct {
	Db         *sql.DB
	Table      string
	Ctx        context.Context
	AutoCreate bool
	Owner      string
}

func (o *Opt) Complete() *Opt {
	if len(o.Table) == 0 {
		o.Table = DefaultTable
	}
	if o.Ctx == nil {
		o.Ctx = context.Background()
	}
	if len(o.Owner) == 0 {
		o.Owner = strings.ReplaceAll(uuid.NewString(), "-", "")
	}
	return o
}

func (o *Opt) Validate() error {
	if o.Db == nil {
		return errors.New("invalid db")
	}

	if len(o.Table) > 255 {
		return errors.New("table name too long(>255)")
	}

	if len(o.Owner) > 255 {
		return errors.New("lock owner too long(>255)")
	}

	if err := o.Db.PingContext(o.Ctx); err != nil {
		return err
	}

	return nil
}
