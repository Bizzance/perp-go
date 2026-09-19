//go:build integration

package service_test

import (
	"context"
	"sync"
	"testing"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/testutil"
)

// 资金费=持仓量*标记价*费率，多头付出、空头收到(费率为负时反过来)。测试里的溢价率都取整齐的值：
// 标记价-指数价=65 -> 溢价率0.001，一个采样周期只有一个采样点时费率就等于溢价率。
// 种子数据里BTCUSDT结算周期8小时，费率上下限±0.0075

const (
	fundingPeriodMs = int64(8 * 3600 * 1000)
	fundingNow1     = fundingPeriodMs*1000 + 5000 // 落在第1000个周期里
	fundingNow2     = fundingNow1 + fundingPeriodMs
)

func (e *engineEnv) setIndex(t *testing.T, symbol, price string) {
	t.Helper()
	if err := e.markPrice.SetIndexPrice(context.Background(), symbol, decimalOf(t, price)); err != nil {
		t.Fatal(err)
	}
}

// 清掉Redis里这两个symbol的资金费率累加器(Redis各测试共用，不清的话会带进别的测试的采样)和指数价
func (e *engineEnv) resetFundingState(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	c := testutil.NewCache(t)
	for _, sym := range []string{testSymbol, "ETHUSDT"} {
		if err := c.ResetFundingAccumulator(ctx, sym); err != nil {
			t.Fatal(err)
		}
		if err := testutil.NewRedisClient(t).Del(ctx, "perpgo:index:"+sym).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

// 设好标记价和指数价，采一次样
func (e *engineEnv) sampleFunding(t *testing.T, mark, index string) {
	t.Helper()
	e.setMark(t, testSymbol, mark)
	e.setIndex(t, testSymbol, index)
	e.funding.SampleOnce(context.Background())
}

func (e *engineEnv) fundingHistoryRows(t *testing.T) []model.FundingRateRecord {
	t.Helper()
	rows, err := e.funding.History(context.Background(), testSymbol, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// a持有BTC多头0.1@65000，b是对手方空头。返回结算前两边的可用余额
func (e *engineEnv) fundingPositions(t *testing.T) (a, b uint64) {
	t.Helper()
	e.resetFundingState(t)
	a = e.newAccount(t, 1, "10000")
	b = e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	return a, b
}

// 正费率：多头付、空头收，金额相等；结算记录、流水、累加器都对
func TestFunding_LongPaysShortReceivesWhenRatePositive(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a, b := e.fundingPositions(t)
	beforeA, beforeB := e.account(t, a).Available, e.account(t, b).Available
	e.sampleFunding(t, "65065", "65000") // 溢价率0.001

	e.funding.SettleIfDue(ctx, fundingNow1)

	// 0.1 * 65065 * 0.001 = 6.5065
	mustDec(t, e.ledgerSum(t, a, model.TxFundingFee), "-6.5065", "多头资金费流水")
	mustDec(t, e.ledgerSum(t, b, model.TxFundingFee), "6.5065", "空头资金费流水")
	mustDec(t, e.account(t, a).Available.Sub(beforeA), "-6.5065", "多头可用余额变化")
	mustDec(t, e.account(t, b).Available.Sub(beforeB), "6.5065", "空头可用余额变化")

	rows := e.fundingHistoryRows(t)
	if len(rows) != 1 {
		t.Fatalf("应该有1条结算记录, got %d", len(rows))
	}
	mustDec(t, rows[0].Rate, "0.001", "记录的费率")
	mustDec(t, rows[0].MarkPrice, "65065", "记录的标记价")
	mustDec(t, rows[0].IndexPrice, "65000", "记录的指数价")
	if rows[0].FundingTime != fundingNow1/fundingPeriodMs*fundingPeriodMs {
		t.Fatalf("结算时间应该对齐到周期边界, got %d", rows[0].FundingTime)
	}

	coin, _ := e.coins.FindBySymbol(ctx, testSymbol)
	mustDec(t, e.funding.EstimateRate(ctx, *coin), "0", "结算后累加器清空，下一周期重新累计")
}

// 负费率(标记价低于指数价)：空头付、多头收
func TestFunding_ShortPaysLongReceivesWhenRateNegative(t *testing.T) {
	e := newEngineEnv(t)
	a, b := e.fundingPositions(t)
	e.sampleFunding(t, "64935", "65000") // 溢价率-0.001

	e.funding.SettleIfDue(context.Background(), fundingNow1)

	// 0.1 * 64935 * 0.001 = 6.4935
	mustDec(t, e.ledgerSum(t, a, model.TxFundingFee), "6.4935", "多头收到")
	mustDec(t, e.ledgerSum(t, b, model.TxFundingFee), "-6.4935", "空头支付")
}

// 一个周期内多次采样取平均(TWAP)：0.001和0.003的均值0.002，按结算时的标记价计费
func TestFunding_RateIsTWAPOfSamples(t *testing.T) {
	e := newEngineEnv(t)
	a, _ := e.fundingPositions(t)
	e.sampleFunding(t, "65065", "65000") // 0.001
	e.sampleFunding(t, "65195", "65000") // 0.003，标记价停在65195

	e.funding.SettleIfDue(context.Background(), fundingNow1)

	// 0.1 * 65195(结算时的标记价) * 0.002 = 13.039
	mustDec(t, e.ledgerSum(t, a, model.TxFundingFee), "-13.039", "按平均费率0.002结算")
	mustDec(t, e.fundingHistoryRows(t)[0].Rate, "0.002", "记录的费率是采样均值")
}

// 溢价率过大时clamp到上限±0.0075
func TestFunding_RateIsClampedToCap(t *testing.T) {
	e := newEngineEnv(t)
	a, _ := e.fundingPositions(t)
	e.sampleFunding(t, "66300", "65000") // 溢价率0.02，超过上限

	e.funding.SettleIfDue(context.Background(), fundingNow1)

	// 0.1 * 66300 * 0.0075 = 49.725
	mustDec(t, e.ledgerSum(t, a, model.TxFundingFee), "-49.725", "费率被夹到0.0075")
	mustDec(t, e.fundingHistoryRows(t)[0].Rate, "0.0075", "记录的费率也是夹后的")
}

// 同一个周期只结算一次：重复调用不会重复扣款；跨到下一个周期才会再结算
func TestFunding_SettlesOncePerPeriod(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a, _ := e.fundingPositions(t)
	e.sampleFunding(t, "65065", "65000")

	e.funding.SettleIfDue(ctx, fundingNow1)
	e.funding.SettleIfDue(ctx, fundingNow1+1000) // 同一个周期里再来一次
	mustDec(t, e.ledgerSum(t, a, model.TxFundingFee), "-6.5065", "同一周期重复调用不能重复结算")
	if n := len(e.fundingHistoryRows(t)); n != 1 {
		t.Fatalf("同一周期只应该有1条记录, got %d", n)
	}

	e.sampleFunding(t, "65065", "65000")
	e.funding.SettleIfDue(ctx, fundingNow2)
	mustDec(t, e.ledgerSum(t, a, model.TxFundingFee), "-13.013", "下一个周期再结算一次(6.5065*2)")
	if n := len(e.fundingHistoryRows(t)); n != 2 {
		t.Fatalf("两个周期应该有2条记录, got %d", n)
	}
}

// 多个实例/goroutine同时到点结算：先占结算记录的唯一键、再转账，只有抢到的那个会转账，
// 不会重复扣款
func TestFunding_ConcurrentSettleTransfersOnce(t *testing.T) {
	e := newEngineEnv(t)
	a, b := e.fundingPositions(t)
	e.sampleFunding(t, "65065", "65000")

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			e.funding.SettleIfDue(context.Background(), fundingNow1)
		}()
	}
	close(start)
	wg.Wait()

	mustDec(t, e.ledgerSum(t, a, model.TxFundingFee), "-6.5065", "并发结算只能转一次")
	mustDec(t, e.ledgerSum(t, b, model.TxFundingFee), "6.5065", "并发结算只能转一次")
	if n := len(e.fundingHistoryRows(t)); n != 1 {
		t.Fatalf("只应该有1条结算记录, got %d", n)
	}
}

// 缺指数价时采样被跳过(不能当0算，会算出一个有偏的巨大溢价)，这个周期费率为0、不转账，但记录照写
func TestFunding_MissingIndexPriceSkipsSampling(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a, _ := e.fundingPositions(t)
	e.setMark(t, testSymbol, "65065") // 只有标记价，没有指数价

	e.funding.SampleOnce(ctx)
	coin, _ := e.coins.FindBySymbol(ctx, testSymbol)
	mustDec(t, e.funding.EstimateRate(ctx, *coin), "0", "没有指数价不应该产生采样")

	e.funding.SettleIfDue(ctx, fundingNow1)
	mustDec(t, e.ledgerSum(t, a, model.TxFundingFee), "0", "费率0不转账")
	rows := e.fundingHistoryRows(t)
	if len(rows) != 1 {
		t.Fatalf("费率0也应该写一条结算记录占住这个周期, got %d", len(rows))
	}
	mustDec(t, rows[0].Rate, "0", "记录的费率")
}

// 没有标记价格没法结算：不写记录、不转账，等标记价格有了下一次tick重试就能结算
func TestFunding_MissingMarkPriceDefersSettlement(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a, _ := e.fundingPositions(t)
	e.sampleFunding(t, "65065", "65000")
	e.clearMark(t, testSymbol)

	e.funding.SettleIfDue(ctx, fundingNow1)
	if n := len(e.fundingHistoryRows(t)); n != 0 {
		t.Fatalf("缺标记价格不应该写结算记录, got %d", n)
	}
	mustDec(t, e.ledgerSum(t, a, model.TxFundingFee), "0", "缺标记价格不转账")

	e.setMark(t, testSymbol, "65065")
	e.funding.SettleIfDue(ctx, fundingNow1)
	mustDec(t, e.ledgerSum(t, a, model.TxFundingFee), "-6.5065", "补上标记价格后重试能结算")
}

// 已经平掉的仓位、没有仓位的账户不收资金费；空的symbol也不出错
func TestFunding_ClosedPositionsAreNotCharged(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a, b := e.fundingPositions(t)
	if _, err := e.db.Exec(`UPDATE positions SET volume = 0, position_margin = 0, status = 'closed' WHERE uid = ?`, a); err != nil {
		t.Fatal(err)
	}
	e.sampleFunding(t, "65065", "65000")

	e.funding.SettleIfDue(ctx, fundingNow1)

	mustDec(t, e.ledgerSum(t, a, model.TxFundingFee), "0", "已平仓的仓位不收资金费")
	// 空头仓位还开着，照常收到
	mustDec(t, e.ledgerSum(t, b, model.TxFundingFee), "6.5065", "还开着的空头照常收到")
	_ = decimal.Zero
}
