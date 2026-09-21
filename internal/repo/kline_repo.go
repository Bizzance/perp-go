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

// 一笔成交要更新的某个周期的那一根K线：周期+这根K线的开盘时间
type KlineBucket struct {
	Interval model.KlineInterval
	OpenTime int64
}

// 一笔成交同时更新好几个周期(1m/5m/15m/1h/4h/1d)各自对应的K线，一条多行
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

// 一次性查回buckets里指定的那几根K线(每个周期各自的open_time不一样，不能
// 用简单的open_time IN(...)，要按(interval,open_time)配对匹配)——WS推送K线快照时，
// UpsertBatch写完之后要把写入后的最新状态读回来推给客户端，用这一条SQL一次查完全部
// 周期，不是每个周期单独查一次
func (r *KlineRepo) FindByBuckets(ctx context.Context, symbol string, buckets []KlineBucket) ([]model.Kline, error) {
	if len(buckets) == 0 {
		return nil, nil
	}
	var sb strings.Builder
	sb.WriteString("SELECT * FROM klines WHERE symbol = ? AND (")
	args := make([]any, 0, len(buckets)*2+1)
	args = append(args, symbol)
	for i, b := range buckets {
		if i > 0 {
			sb.WriteString(" OR ")
		}
		sb.WriteString("(`interval` = ? AND open_time = ?)")
		args = append(args, b.Interval, b.OpenTime)
	}
	sb.WriteString(")")
	var rows []model.Kline
	err := r.db.SelectContext(ctx, &rows, sb.String(), args...)
	return rows, err
}

// 用外部行情的K线整根覆盖：存在就把开高低收、成交量、成交笔数都换成这一批的值(不是跟已有的合并)，不存在就新建。
// 跟UpsertBatch的区别：那个是"一笔成交并进这根K线"(最高取GREATEST、成交量累加)，这个是"这根K线就是这样"，
// 重复执行结果不变(幂等)。candles都是同一个symbol、同一个interval
func (r *KlineRepo) ReplaceBatch(ctx context.Context, symbol string, interval model.KlineInterval, candles []model.Kline, updateTime int64) error {
	if len(candles) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO klines (symbol, `interval`, open_time, open, high, low, close, volume, trade_count, update_time) VALUES ")
	args := make([]any, 0, len(candles)*10)
	for i, k := range candles {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, symbol, interval, k.OpenTime, k.Open, k.High, k.Low, k.Close, k.Volume, k.TradeCount, updateTime)
	}
	sb.WriteString(" ON DUPLICATE KEY UPDATE " +
		"open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close), " +
		"volume = VALUES(volume), trade_count = VALUES(trade_count), update_time = VALUES(update_time)")
	_, err := r.db.ExecContext(ctx, sb.String(), args...)
	return err
}

// 这个symbol、这个周期里指定开盘时间的那几根K线(没有的不返回)，POST /kline/sync用它判断哪些K线真的变了
func (r *KlineRepo) FindByOpenTimes(ctx context.Context, symbol string, interval model.KlineInterval, openTimes []int64) ([]model.Kline, error) {
	if len(openTimes) == 0 {
		return nil, nil
	}
	query, args, err := sqlx.In("SELECT * FROM klines WHERE symbol = ? AND `interval` = ? AND open_time IN (?)", symbol, interval, openTimes)
	if err != nil {
		return nil, err
	}
	var rows []model.Kline
	err = r.db.SelectContext(ctx, &rows, query, args...)
	return rows, err
}

// 最近limit根K线，按开盘时间升序返回(从旧到新，画图/回放的常见习惯)
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
