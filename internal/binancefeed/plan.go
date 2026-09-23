package binancefeed

import (
	"sort"

	"github.com/shopspring/decimal"
)

// 同步的纯逻辑：把币安的一侧盘口整理成"期望挂的档位"，再跟我们订单簿里系统账户已经挂着的委托做差异，
// 算出要撤哪些、要挂哪些。不碰网络，方便单独测试。

// 币安盘口的前n档(levels已经是最优价在前)：价格和数量都原样，数量向下取到合约允许的小数位、
// 再向下取到数量步长的整数倍(step为0表示合约不校验步长)，取整后低于最小下单量的档位不挂
func DesiredLevels(levels []Level, n int, qtyDP int32, step, minVolume decimal.Decimal) []Level {
	var out []Level
	for _, l := range levels {
		if len(out) == n {
			break
		}
		q := l.Qty.Truncate(qtyDP)
		if step.Sign() > 0 {
			q = q.Div(step).Floor().Mul(step)
		}
		if q.Sign() <= 0 || q.LessThan(minVolume) {
			continue
		}
		out = append(out, Level{Price: l.Price, Qty: q})
	}
	return out
}

// 系统当前挂在订单簿里的一笔委托
type LiveOrder struct {
	ID        uint64
	Price     decimal.Decimal
	Amount    decimal.Decimal // 下单时的数量
	Remaining decimal.Decimal // 还没成交的数量
}

type Placement struct {
	Price decimal.Decimal
	Qty   decimal.Decimal
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
func Plan(desired []Level, existing []LiveOrder) (cancels []uint64, places []Placement) {
	want := make(map[string]Level, len(desired))
	for _, d := range desired {
		want[d.Price.String()] = d
	}
	kept := make(map[string]bool, len(desired))

	sorted := append([]LiveOrder(nil), existing...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	for _, o := range sorted {
		key := o.Price.String()
		_, ok := want[key]
		switch {
		case !ok:
			cancels = append(cancels, o.ID)
		case kept[key]:
			cancels = append(cancels, o.ID) // 这个价位已经有一笔保留了，多余的撤掉
		case o.Remaining.Mul(decimal.NewFromInt(2)).LessThan(o.Amount):
			cancels = append(cancels, o.ID) // 被吃掉大半，撤掉后面重挂
		default:
			kept[key] = true
		}
	}
	for _, d := range desired {
		if !kept[d.Price.String()] && d.Qty.Sign() > 0 {
			places = append(places, Placement{Price: d.Price, Qty: d.Qty})
		}
	}
	return cancels, places
}

// 已经挂着的委托里最低/最高的价格，没有委托时返回ok=false
func MinLivePrice(orders []LiveOrder) (decimal.Decimal, bool) {
	var best decimal.Decimal
	for i, o := range orders {
		if i == 0 || o.Price.LessThan(best) {
			best = o.Price
		}
	}
	return best, len(orders) > 0
}

func MaxLivePrice(orders []LiveOrder) (decimal.Decimal, bool) {
	var best decimal.Decimal
	for i, o := range orders {
		if i == 0 || o.Price.GreaterThan(best) {
			best = o.Price
		}
	}
	return best, len(orders) > 0
}

// 只保留价格严格低于limit的补挂档位(买单不能碰到还挂着的卖单)，没有limit(ok=false)就全保留
func PlacesBelow(places []Placement, limit decimal.Decimal, ok bool) []Placement {
	if !ok {
		return places
	}
	var out []Placement
	for _, p := range places {
		if p.Price.LessThan(limit) {
			out = append(out, p)
		}
	}
	return out
}

// 只保留价格严格高于limit的补挂档位(卖单不能碰到还挂着的买单)
func PlacesAbove(places []Placement, limit decimal.Decimal, ok bool) []Placement {
	if !ok {
		return places
	}
	var out []Placement
	for _, p := range places {
		if p.Price.GreaterThan(limit) {
			out = append(out, p)
		}
	}
	return out
}
