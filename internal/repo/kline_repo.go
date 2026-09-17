package repo

import (
	"context"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

type KlineRepo struct{ db *sqlx.DB }

func NewKlineRepo(db *sqlx.DB) *KlineRepo { return &KlineRepo{db: db} }

// KlineBucket 一笔成交要更新的某个周期的那一根K线：周期+这根K线的开盘时间
type KlineBucket struct {
	Interval model.KlineInterval
	OpenTime int64
}

// UpsertBatch 一笔成交同时更新好几个周期(1m/5m/15m/1h/4h/1d)各自对应的K线，一条多行
// INSERT ... ON DUPLICATE KEY UPDATE原子完成，不是逐个周期分别发一条UPSERT——每笔成交都要
// 更新全部周期，逐个发送等于让成交结算的热路径每笔成交多等5次DB往返，多行合并成一条SQL
// 只需要1次往返。MySQL的多行UPSERT里VALUES(列名)是按每一行各自的待插入值算的，不会跟
// 同一条语句里其它行的值混在一起，已经用真实MySQL实例验证过
func (r *KlineRepo) UpsertBatch(ctx context.Context, symbol string, buckets []KlineBucket, price, volume decimal.Decimal, updateTime int64) error {
	if len(buckets) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO klines (symbol, `interval`, open_time, open, high, low, close, volume, trade_count, update_time) VALUES ")
	args := make([]any, 0, len(buckets)*9)
	for i, b := range buckets {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, 1, ?)")
		args = append(args, symbol, b.Interval, b.OpenTime, price, price, price, price, volume, updateTime)
	}
	sb.WriteString(" ON DUPLICATE KEY UPDATE " +
		"high = GREATEST(high, VALUES(high)), " +
		"low = LEAST(low, VALUES(low)), " +
		"close = VALUES(close), " +
		"volume = volume + VALUES(volume), " +
		"trade_count = trade_count + 1, " +
		"update_time = VALUES(update_time)")
	_, err := r.db.ExecContext(ctx, sb.String(), args...)
	return err
}

// FindRecent 最近limit根K线，按开盘时间升序返回(从旧到新，画图/回放的常见习惯)
func (r *KlineRepo) FindRecent(ctx context.Context, symbol string, interval model.KlineInterval, limit int) ([]model.Kline, error) {
	var rows []model.Kline
	err := r.db.SelectContext(ctx, &rows,
		"SELECT * FROM klines WHERE symbol = ? AND `interval` = ? ORDER BY open_time DESC LIMIT ?",
		symbol, interval, limit)
	if err != nil {
		return nil, err
	}
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows, nil
}
