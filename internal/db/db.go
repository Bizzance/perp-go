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
