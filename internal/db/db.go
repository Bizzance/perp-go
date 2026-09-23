package db

import (
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func Connect(dsn string) (*sqlx.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse mysql dsn: %w", err)
	}
	// 必须开：账户/仓位/委托这些表大量用"UPDATE ... WHERE 匹配期望的旧值"做乐观并发控制，
	// 关心的是"WHERE条件匹配到了行"，不是"某一列的值真的发生了变化"。按MySQL默认的
	// "受影响行数=值发生变化的行数"语义，SET子句算出来的新值恰好等于旧值时(比如
	// 结算时delta刚好是0，这是完全正常的情况，不是并发冲突)，RowsAffected()会返回0，
	// 被误判成"没匹配到、乐观锁冲突"
	cfg.ClientFoundRows = true
	conn, err := sqlx.Connect("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("connect mysql: %w", err)
	}
	conn.SetMaxOpenConns(20)
	conn.SetMaxIdleConns(10)
	return conn, nil
}
