create table locky_mysql_distributed_lock
(
    lock_id      varchar(255)    not null primary key,
    lock_owner     varchar(255)    not null,
    lock_timestamp bigint unsigned not null,
    lock_ttl       int unsigned    not null
) charset = utf8mb3;
