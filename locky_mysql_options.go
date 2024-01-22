package locky

import (
	"context"
	"database/sql"
	"errors"
	"math/rand"
	"time"
)

type Opt struct {
	Db         *sql.DB
	Table      string
	Ctx        context.Context
	AutoCreate bool
	Owner      string
}

func (o *Opt) DefaultAndValidate() error {
	if o.Db == nil {
		return errors.New("invalid db")
	}

	if len(o.Table) == 0 {
		o.Table = DefaultTable
	}

	if len(o.Table) > 255 {
		return errors.New("table name too long(>255)")
	}

	if o.Ctx == nil {
		o.Ctx = context.Background()
	}

	if len(o.Owner) == 0 {
		o.Owner = generateRandomString(16)
	}

	if len(o.Owner) > 255 {
		return errors.New("lock owner too long(>255)")
	}

	if err := o.Db.PingContext(o.Ctx); err != nil {
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
