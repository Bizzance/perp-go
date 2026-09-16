// Package db 提供MySQL连接——用sqlx而不是GORM，原子条件UPDATE("UPDATE...WHERE available>=?")
// 这个并发安全模式要求精确控制SQL语句本身，跟ORM的抽象合不来，Java版当初用手写JPQL也是同一个
// 理由(见plan文件)。
package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

func Connect(dsn string) (*sqlx.DB, error) {
	conn, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("connect mysql: %w", err)
	}
	conn.SetMaxOpenConns(20)
	conn.SetMaxIdleConns(10)
	return conn, nil
}
