package main

import (
	"sort"

	"github.com/shopspring/decimal"
)

// 做市的纯逻辑：把币安盘口聚合成一组价格档，再跟我们订单簿里已有的系统挂单做差异，算出要撤哪些、
// 要挂哪些。这部分不碰网络，方便单独测试。

type level struct {
	price decimal.Decimal
	qty   decimal.Decimal
}

var (
	one = decimal.NewFromInt(1)
	ten = decimal.NewFromInt(10)
)

// 不小于v的最小的"1、2、5乘以10的整数次幂"，用来给没配置聚合粒度的合约选一个合适的价格桶大小
// (BTC约80000 * 0.00005 = 4 -> 5，ETH约2300 * 0.00005 = 0.115 -> 0.2)
func niceStep(v decimal.Decimal) decimal.Decimal {
	if v.Sign() <= 0 {
		return one
	}
	mag := one
	for mag.GreaterThan(v) { // v < 1：先往下找到不大于v的10的幂
		mag = mag.Div(ten)
	}
	for mag.Mul(ten).LessThanOrEqual(v) { // 再往上找到最大的不大于v的10的幂
		mag = mag.Mul(ten)
	}
	for _, m := range []int64{1, 2, 5, 10} {
		if s := mag.Mul(decimal.NewFromInt(m)); s.GreaterThanOrEqual(v) {
			return s
		}
	}
	return mag.Mul(ten)
}

// 把币安的一侧盘口(levels必须已经按"最优价在前"排好序)按价格桶聚合，取最靠近成交价的k个桶。
// 买盘向下取整到桶边界、卖盘向上取整，这样买一桶一定低于卖一桶，不会跟我们自己的另一侧挂单交叉。
// 同一个桶里的数量累加。最优价所在的桶只包含从最优价到桶边界这一段的数量，不会把桶外的算进来
func aggregate(levels []level, step decimal.Decimal, isBid bool, k int) []level {
	if step.Sign() <= 0 || k <= 0 {
		return nil
	}
	var out []level
	for _, l := range levels {
		var bucket decimal.Decimal
		q := l.price.Div(step)
		if isBid {
			bucket = q.Floor().Mul(step)
		} else {
			bucket = q.Ceil().Mul(step)
		}
		if n := len(out); n > 0 && out[n-1].price.Equal(bucket) {
			out[n-1].qty = out[n-1].qty.Add(l.qty)
			continue
		}
		if len(out) == k {
			break
		}
		out = append(out, level{price: bucket, qty: l.qty})
	}
	return out
}

// 币安的数量按比例缩放，向下取到合约允许的小数位；低于最小下单量的档位不挂(返回0)
func scaleQty(q, scale decimal.Decimal, dp int32, min decimal.Decimal) decimal.Decimal {
	v := q.Mul(scale).Truncate(dp)
	if v.LessThan(min) {
		return decimal.Zero
	}
	return v
}

// 系统当前挂在订单簿里的一笔委托
type liveOrder struct {
	id        string
	price     decimal.Decimal
	amount    decimal.Decimal // 下单时的数量
	remaining decimal.Decimal // 还没成交的数量
}

type placement struct {
	price decimal.Decimal
	qty   decimal.Decimal
}

// 期望的一组档位 vs 已经挂着的委托：
//   - 价格不在期望里的撤掉
//   - 期望的价格上已经有委托、且剩余量还够(不低于它下单时数量的一半)就保留，多出来的重复挂单撤掉；
//     剩余量不够(被用户吃掉了大半)就撤掉重挂，补回期望量。这里跟"下单时的数量"比，不跟当前期望量比：
//     币安某个价位的挂单量一波动，期望量就跟着变，跟期望量比的话会天天撤了重挂
//   - 期望的价格上没有可用委托的，新挂
//
// 只在档位真的变了或者被吃掉时才动，不会为了数量的小幅波动天天撤了重挂：那样每秒几十个请求，
// 还会让订单簿里的排队位置一直丢
func plan(desired []level, existing []liveOrder) (cancels []string, places []placement) {
	want := make(map[string]level, len(desired))
	for _, d := range desired {
		want[d.price.String()] = d
	}
	kept := make(map[string]bool, len(desired))

	sorted := append([]liveOrder(nil), existing...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].id < sorted[j].id })
	for _, o := range sorted {
		key := o.price.String()
		_, ok := want[key]
		switch {
		case !ok:
			cancels = append(cancels, o.id)
		case kept[key]:
			cancels = append(cancels, o.id) // 这个价位已经有一笔保留了，多余的撤掉
		case o.remaining.Mul(decimal.NewFromInt(2)).LessThan(o.amount):
			cancels = append(cancels, o.id) // 被吃掉大半，撤掉后面重挂
		default:
			kept[key] = true
		}
	}
	for _, d := range desired {
		if !kept[d.price.String()] && d.qty.Sign() > 0 {
			places = append(places, placement{price: d.price, qty: d.qty})
		}
	}
	return cancels, places
}

// 行情情景的偏移往目标推进一步：每一步最多动maxStep个百分点。开仓限价单有价格保护带(偏离标记价超过5%会被拒)，
// 做市的报价和标记价必须一步步走，一步跳很远的话挂单全被拒、对敲也做不了，标记价就永远追不上
func rampOffset(cur, target, maxStep decimal.Decimal) decimal.Decimal {
	diff := target.Sub(cur)
	if diff.Abs().LessThanOrEqual(maxStep) {
		return target
	}
	if diff.Sign() > 0 {
		return cur.Add(maxStep)
	}
	return cur.Sub(maxStep)
}

// 给一侧盘口的每个价位乘上系数(1 + 偏移%/100)，数量不变
func shiftLevels(levels []level, factor decimal.Decimal) []level {
	out := make([]level, len(levels))
	for i, l := range levels {
		out[i] = level{price: l.price.Mul(factor), qty: l.qty}
	}
	return out
}

// 偏移百分比对应的价格系数
func offsetFactor(pct decimal.Decimal) decimal.Decimal {
	return one.Add(pct.Div(decimal.NewFromInt(100)))
}

// 已经挂着的委托里最低/最高的价格，没有委托时返回ok=false
func minLivePrice(orders []liveOrder) (decimal.Decimal, bool) {
	var best decimal.Decimal
	for i, o := range orders {
		if i == 0 || o.price.LessThan(best) {
			best = o.price
		}
	}
	return best, len(orders) > 0
}

func maxLivePrice(orders []liveOrder) (decimal.Decimal, bool) {
	var best decimal.Decimal
	for i, o := range orders {
		if i == 0 || o.price.GreaterThan(best) {
			best = o.price
		}
	}
	return best, len(orders) > 0
}

// 只保留价格严格低于limit的补挂档位(买单不能碰到还挂着的卖单)，没有limit(ok=false)就全保留
func placesBelow(places []placement, limit decimal.Decimal, ok bool) []placement {
	if !ok {
		return places
	}
	var out []placement
	for _, p := range places {
		if p.price.LessThan(limit) {
			out = append(out, p)
		}
	}
	return out
}

// 只保留价格严格高于limit的补挂档位(卖单不能碰到还挂着的买单)
func placesAbove(places []placement, limit decimal.Decimal, ok bool) []placement {
	if !ok {
		return places
	}
	var out []placement
	for _, p := range places {
		if p.price.GreaterThan(limit) {
			out = append(out, p)
		}
	}
	return out
}
