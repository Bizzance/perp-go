package repo

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestIsDuplicateKeyAndIsDeadlock(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	dead := &mysql.MySQLError{Number: 1213, Message: "Deadlock found"}
	other := &mysql.MySQLError{Number: 1146, Message: "no such table"}

	if !IsDuplicateKey(dup) || IsDuplicateKey(dead) || IsDuplicateKey(other) || IsDuplicateKey(nil) {
		t.Error("IsDuplicateKey只应该识别1062")
	}
	if !IsDeadlock(dead) || IsDeadlock(dup) || IsDeadlock(other) || IsDeadlock(nil) {
		t.Error("IsDeadlock只应该识别1213")
	}
	// 被fmt.Errorf包装过的错误也要能识别(errors.As沿着%w链找)
	if !IsDuplicateKey(fmt.Errorf("insert: %w", dup)) || !IsDeadlock(fmt.Errorf("tx: %w", dead)) {
		t.Error("包装过的错误也应该被识别")
	}
	if IsDuplicateKey(errors.New("plain")) {
		t.Error("普通错误不是唯一索引冲突")
	}
}
