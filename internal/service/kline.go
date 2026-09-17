package service

import (
	"context"
	"log"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/repo"
)

// KlineService 每笔成交实时更新K线——不是查询时现场从trades表聚合。运行在contract-engine，
// 挂在EngineService.settleOneFill后面，跟成交结算走同一个调用路径，见docs/kline.md
type KlineService struct {
	klines *repo.KlineRepo
}

func NewKlineService(klines *repo.KlineRepo) *KlineService {
	return &KlineService{klines: klines}
}

// RecordTrade 用一笔成交更新这个symbol全部周期(1m/5m/15m/1h/4h/1d)各自对应的那一根K线，
// 一次批量UPSERT(见KlineRepo.UpsertBatch)，不是每个周期单独发一次DB请求——成交结算是热
// 路径，串行等6次DB往返会白白拖慢撮合/结算的吞吐。更新失败只记日志、不返回error——K线是
// 行情展示用的辅助数据，不影响交易主链路的正确性，没必要因为这个让整笔成交结算失败
func (s *KlineService) RecordTrade(ctx context.Context, symbol string, price, volume decimal.Decimal, tradeTime int64) {
	buckets := make([]repo.KlineBucket, len(model.AllKlineIntervals))
	for i, interval := range model.AllKlineIntervals {
		bucketMillis := model.KlineIntervalMillis[interval]
		buckets[i] = repo.KlineBucket{Interval: interval, OpenTime: (tradeTime / bucketMillis) * bucketMillis}
	}
	if err := s.klines.UpsertBatch(ctx, symbol, buckets, price, volume, tradeTime); err != nil {
		log.Printf("[ERROR] 更新K线失败, symbol=%s: %v", symbol, err)
	}
}
