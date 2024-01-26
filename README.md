# Locky

Locky implements the distributed lock with different databases, so you don't have to do it again. Current database support:
* Mysql

## Features
* No transaction
* Lock TTL 
* Lock owner identification
* Lease(TODO)

## Examples
```go
db, err := sql.Open("mysql", "root:root@/test")
if err != nil {
	panic(err)
}

lock, err := NewMysqlDistributedLock(Opt{
	Db:         db,
	Table:      "custom_lock_table",
	AutoCreate: true,
})

lockId := "lock2024"
locked, err := lock.Lock(lockId, 3*time.Second)
if err != nil {
	panic(err)
}

if locked {
	defer func() {
		if err := lock.Unlock(lockId); err != nil {
			log.Println(err)
		}
	}()
	// do your stuff
}
```