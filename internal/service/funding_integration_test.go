//go:build integration

package service_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/matching"
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
		resetPriceKeys(t, sym)
	}
}

type bookLevel struct{ price, volume string }

// 把这个symbol的订单簿换成指定的几档(先撤掉上一次摆的)，每边最优价在前
func (e *engineEnv) setBook(t *testing.T, symbol string, bids, asks []bookLevel) {
	t.Helper()
	if e.bookIDs == nil {
		e.bookIDs = map[string][]uint64{}
	}
	b := e.book.BookFor(symbol)
	for _, id := range e.bookIDs[symbol] {
		b.Cancel(id)
	}
	e.bookIDs[symbol] = nil
	place := func(dir matching.Direction, levels []bookLevel) {
		for _, l := range levels {
			id := e.id()
			ok := b.Rest(&matching.RestingOrder{OrderID: id, UID: id, Direction: dir,
				Price: decimalOf(t, l.price), Remaining: decimalOf(t, l.volume), EntryTime: int64(id)})
			if !ok {
				t.Fatalf("挂单失败: %+v", l)
			}
			e.bookIDs[symbol] = append(e.bookIDs[symbol], id)
		}
	}
	place(matching.Buy, bids)
	place(matching.Sell, asks)
}

// 设好标记价、指数价，并摆一个订单簿让这一刻的溢价恰好等于(标记价-指数价)/指数价，采一次样：
// 溢价用冲击价格算，所以这里买卖各摆一档足够深(65万名义价值，远超冲击名义金额)的单子，冲击价格就是这一档的价格——
// mark>=index时买价=mark、卖价=mark+1(都不低于指数价，溢价=(mark-index)/index)；
// mark<index时买价=mark-1、卖价=mark(都低于指数价，溢价=(mark-index)/index)。
// 标记价本身不参与溢价的计算，设它是因为结算要用标记价算资金费的名义价值
func (e *engineEnv) sampleFunding(t *testing.T, mark, index string) {
	t.Helper()
	e.setMark(t, testSymbol, mark)
	e.setIndex(t, testSymbol, index)
	m := decimalOf(t, mark)
	if m.GreaterThanOrEqual(decimalOf(t, index)) {
		e.setBook(t, testSymbol, []bookLevel{{mark, "10"}}, []bookLevel{{m.Add(decimal.NewFromInt(1)).String(), "10"}})
	} else {
		e.setBook(t, testSymbol, []bookLevel{{m.Sub(decimal.NewFromInt(1)).String(), "10"}}, []bookLevel{{mark, "10"}})
	}
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

// ---- 溢价用冲击价格算(币安口径)，见FundingService.SampleOnce ----
// 期望值都按定义手算：冲击价=按名义金额(种子数据里默认10000 USDT)从最优价吃盘口的平均价，
// 溢价=[max(0,冲击买价-指数价)-max(0,指数价-冲击卖价)]/指数价

// 读这个symbol当前资金费率周期的累加器：(样本和, 样本数)
func (e *engineEnv) fundingAccumulator(t *testing.T, symbol string) (decimal.Decimal, int64) {
	t.Helper()
	sum, count, err := testutil.NewCache(t).GetFundingAccumulator(context.Background(), symbol)
	if err != nil {
		t.Fatal(err)
	}
	return sum, count
}

// 累加器里是float64，比较时留一点容差
func mustNear(t *testing.T, got, want decimal.Decimal, what string) {
	t.Helper()
	if got.Sub(want).Abs().GreaterThan(decimal.RequireFromString("0.000000001")) {
		t.Fatalf("%s: got %s, want %s", what, got, want)
	}
}

// 冲击价格要走多档：买盘先吃65100档的0.1个(6510)，剩下3490吃65000档，均价10000/(0.1+3490/65000)=65065.065...；
// 卖盘65200一档足够深。溢价=(65065.065...-65000)/65000=1/999。标记价设成完全无关的70000：溢价不看标记价
func TestFunding_PremiumIsBuiltFromImpactPrices(t *testing.T) {
	e := newEngineEnv(t)
	e.resetFundingState(t)
	e.setMark(t, testSymbol, "70000")
	e.setIndex(t, testSymbol, "65000")
	e.setBook(t, testSymbol, []bookLevel{{"65100", "0.1"}, {"65000", "100"}}, []bookLevel{{"65200", "100"}})
	e.funding.SampleOnce(context.Background())

	sum, count := e.fundingAccumulator(t, testSymbol)
	if count != 1 {
		t.Fatalf("应该采到1个样本, got %d", count)
	}
	mustNear(t, sum, decimal.NewFromInt(1).Div(decimal.NewFromInt(999)), "溢价=1/999")
}

// 买一被拉到指数价上方1%(65650)，但只挂了0.01个(656.5名义价值，不到冲击名义金额的7%)：
// 冲击买价=10000/(0.01+9343.5/65000)=65042.277...，溢价=13/19987≈0.065%。
// 如果直接用买一卖一中价((65650+65700)/2)算基差是1.04%，是它的16倍——冲击价格要影响它得摆出接近名义金额那么大的单子
func TestFunding_TinyOrderAtTheTouchMovesPremiumOnlyInProportion(t *testing.T) {
	e := newEngineEnv(t)
	e.resetFundingState(t)
	e.setMark(t, testSymbol, "65000")
	e.setIndex(t, testSymbol, "65000")
	e.setBook(t, testSymbol, []bookLevel{{"65650", "0.01"}, {"65000", "100"}}, []bookLevel{{"65700", "100"}})
	e.funding.SampleOnce(context.Background())

	sum, count := e.fundingAccumulator(t, testSymbol)
	if count != 1 {
		t.Fatalf("应该采到1个样本, got %d", count)
	}
	mustNear(t, sum, decimal.NewFromInt(13).Div(decimal.NewFromInt(19987)), "溢价=13/19987")
}

// 没有人操纵的正常盘口：买价=指数价、卖价高于指数价(65100)，指数价不在买卖价之外，溢价0
// (买一卖一中价基差会算出0.077%)
func TestFunding_IndexInsideTheSpreadGivesZeroPremium(t *testing.T) {
	e := newEngineEnv(t)
	e.resetFundingState(t)
	e.setIndex(t, testSymbol, "65000")
	e.setBook(t, testSymbol, []bookLevel{{"65000", "100"}}, []bookLevel{{"65100", "100"}})
	e.funding.SampleOnce(context.Background())

	sum, count := e.fundingAccumulator(t, testSymbol)
	if count != 1 || !sum.IsZero() {
		t.Fatalf("应该采到1个溢价为0的样本, got sum=%s count=%d", sum, count)
	}
}

// 买卖价都低于指数价：溢价取卖价这一侧，-(65000-64900)/65000=-1/650
func TestFunding_NegativePremiumWhenBookIsBelowIndex(t *testing.T) {
	e := newEngineEnv(t)
	e.resetFundingState(t)
	e.setIndex(t, testSymbol, "65000")
	e.setBook(t, testSymbol, []bookLevel{{"64800", "100"}}, []bookLevel{{"64900", "100"}})
	e.funding.SampleOnce(context.Background())

	sum, count := e.fundingAccumulator(t, testSymbol)
	if count != 1 {
		t.Fatalf("应该采到1个样本, got %d", count)
	}
	mustNear(t, sum, decimal.NewFromInt(-1).Div(decimal.NewFromInt(650)), "溢价=-1/650")
}

// 盘口不够深(任何一侧的名义价值不到冲击名义金额)就不采样，不能当0处理；买卖两侧都要够
func TestFunding_BookTooThinSkipsSampling(t *testing.T) {
	e := newEngineEnv(t)
	e.resetFundingState(t)
	e.setIndex(t, testSymbol, "65000")
	ctx := context.Background()

	// 买盘0.1个*65000=6500 < 10000
	e.setBook(t, testSymbol, []bookLevel{{"65000", "0.1"}}, []bookLevel{{"65100", "100"}})
	e.funding.SampleOnce(ctx)
	// 卖盘0.1个*65100=6510 < 10000
	e.setBook(t, testSymbol, []bookLevel{{"65000", "100"}}, []bookLevel{{"65100", "0.1"}})
	e.funding.SampleOnce(ctx)
	// 单边盘口
	e.setBook(t, testSymbol, []bookLevel{{"65000", "100"}}, nil)
	e.funding.SampleOnce(ctx)
	// 空盘口
	e.setBook(t, testSymbol, nil, nil)
	e.funding.SampleOnce(ctx)
	if _, count := e.fundingAccumulator(t, testSymbol); count != 0 {
		t.Fatalf("盘口不够深时不该采样, got %d个样本", count)
	}

	// 两侧都够了：买盘62500*0.16恰好10000，恰好等于冲击名义金额也算够；卖盘65100*0.16=10416
	e.setBook(t, testSymbol, []bookLevel{{"62500", "0.16"}}, []bookLevel{{"65100", "0.16"}})
	e.funding.SampleOnce(ctx)
	if _, count := e.fundingAccumulator(t, testSymbol); count != 1 {
		t.Fatalf("两侧都够深应该采样, got %d个样本", count)
	}
}

// 冲击名义金额是每个合约自己的配置：同一个盘口，金额调大到吃不下就采不到，调小就采得到；
// 配成0表示这个合约不采样
func TestFunding_ImpactNotionalIsPerContractConfig(t *testing.T) {
	e := newEngineEnv(t)
	e.resetFundingState(t)
	e.setIndex(t, testSymbol, "65000")
	ctx := context.Background()
	e.setBook(t, testSymbol, []bookLevel{{"65000", "0.2"}}, []bookLevel{{"65100", "0.2"}}) // 每侧13000左右
	setNotional := func(v string) {
		if _, err := e.db.Exec(`UPDATE coins SET funding_impact_notional = ? WHERE symbol = ?`, v, testSymbol); err != nil {
			t.Fatal(err)
		}
	}

	setNotional("20000")
	e.funding.SampleOnce(ctx)
	if _, count := e.fundingAccumulator(t, testSymbol); count != 0 {
		t.Fatalf("名义金额20000超过盘口深度，不该采样, got %d", count)
	}
	setNotional("5000")
	e.funding.SampleOnce(ctx)
	if _, count := e.fundingAccumulator(t, testSymbol); count != 1 {
		t.Fatalf("名义金额5000吃得下，应该采样, got %d", count)
	}
	setNotional("0")
	e.funding.SampleOnce(ctx)
	if _, count := e.fundingAccumulator(t, testSymbol); count != 1 {
		t.Fatalf("配成0不采样，样本数不该增加, got %d", count)
	}
}

// 指数价断供(超过30秒没更新)：不采样，恢复后继续
func TestFunding_StaleIndexSkipsSampling(t *testing.T) {
	e := newEngineEnv(t)
	e.resetFundingState(t)
	clock := e.useFakeClock(t)
	ctx := context.Background()
	e.setIndex(t, testSymbol, "65000")
	e.setBook(t, testSymbol, []bookLevel{{"65100", "100"}}, []bookLevel{{"65200", "100"}})

	clock.advance(31 * time.Second)
	e.funding.SampleOnce(ctx)
	if _, count := e.fundingAccumulator(t, testSymbol); count != 0 {
		t.Fatalf("指数价陈旧不该采样, got %d", count)
	}
	e.setIndex(t, testSymbol, "65000") // 喂价恢复
	e.funding.SampleOnce(ctx)
	if _, count := e.fundingAccumulator(t, testSymbol); count != 1 {
		t.Fatalf("喂价恢复后应该采样, got %d", count)
	}
}

// 分片部署：只有拥有这个symbol的引擎实例采样，别的实例的本地订单簿是空的、采不出有意义的溢价
func TestFunding_OnlyTheOwningEngineSamples(t *testing.T) {
	e := newEngineEnv(t)
	e.resetFundingState(t)
	ctx := context.Background()
	e.setIndex(t, testSymbol, "65000")

	e.restartEngineOwning(t, []string{"ETHUSDT"}) // 这个实例不负责BTCUSDT
	e.setBook(t, testSymbol, []bookLevel{{"65100", "100"}}, []bookLevel{{"65200", "100"}})
	e.funding.SampleOnce(ctx)
	if _, count := e.fundingAccumulator(t, testSymbol); count != 0 {
		t.Fatalf("不负责的symbol不该采样, got %d", count)
	}

	e.restartEngineOwning(t, []string{testSymbol})
	e.setBook(t, testSymbol, []bookLevel{{"65100", "100"}}, []bookLevel{{"65200", "100"}})
	e.funding.SampleOnce(ctx)
	if _, count := e.fundingAccumulator(t, testSymbol); count != 1 {
		t.Fatalf("负责的symbol应该采样, got %d", count)
	}
}

// 端到端：用冲击价格采到的溢价，结算出的资金费率和划转金额跟手算一致。
// 溢价=1/999；0.1个的仓位、标记价65195：多头付出的资金费=0.1*65195/999=6519.5/999≈6.526026。
// 记录进funding_rate_history的费率列是DECIMAL(10,6)，所以是0.001001；划转用的是内存里没截断的费率
func TestFunding_SettlementUsesImpactPremium(t *testing.T) {
	e := newEngineEnv(t)
	a, _ := e.fundingPositions(t)
	e.setMark(t, testSymbol, "65195")
	e.setIndex(t, testSymbol, "65000")
	e.setBook(t, testSymbol, []bookLevel{{"65100", "0.1"}, {"65000", "100"}}, []bookLevel{{"65200", "100"}})
	e.funding.SampleOnce(context.Background())

	e.funding.SettleIfDue(context.Background(), fundingNow1)

	mustDec(t, e.fundingHistoryRows(t)[0].Rate, "0.001001", "记录的费率是冲击价格算出的溢价")
	want := decimal.RequireFromString("6519.5").Div(decimal.NewFromInt(999)).Neg()
	if got := e.ledgerSum(t, a, model.TxFundingFee); got.Sub(want).Abs().GreaterThan(decimal.RequireFromString("0.000001")) {
		t.Fatalf("多头付出的资金费 = %s, want 约%s", got, want)
	}
}
