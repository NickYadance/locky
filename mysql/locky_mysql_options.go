package mysql

import (
	"database/sql"
	"errors"
	"math/rand"
	"time"
)

type Opt struct {
	Db         *sql.DB
	Table      string
	AutoCreate bool
	Owner      string
}

func (o *Opt) Complete() *Opt {
	if len(o.Table) == 0 {
		o.Table = DefaultTable
	}
	if len(o.Owner) == 0 {
		o.Owner = generateRandomString(16)
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

	if err := o.Db.Ping(); err != nil {
		return err
	}

	return nil
}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	randomString := make([]byte, length)
	rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < length; i++ {
		randomString[i] = charset[rand.Intn(len(charset))]
	}

	return string(randomString)
}
