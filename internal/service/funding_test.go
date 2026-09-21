package service

import "testing"

// 溢价指数 = [max(0, 冲击买价-指数价) - max(0, 指数价-冲击卖价)] / 指数价，期望值都按定义手算(指数价100)
func TestPremiumFromImpact(t *testing.T) {
	cases := []struct {
		name     string
		bid, ask string
		want     string
	}{
		{"买卖价都高于指数价：溢价为正，取买价(101-100)/100", "101", "102", "0.01"},
		{"买卖价都低于指数价：溢价为负，取卖价-(100-99.5)/100", "99", "99.5", "-0.005"},
		{"指数价夹在买卖价之间：溢价0", "99", "101", "0"},
		{"买卖价都等于指数价：溢价0", "100", "100", "0"},
		{"买价恰好等于指数价、卖价更高：溢价0", "100", "102", "0"},
		{"买价更低、卖价恰好等于指数价：溢价0", "98", "100", "0"},
		{"买卖价相同且高于指数价", "100.5", "100.5", "0.005"},
		{"极端偏离不在这里夹(结算时才夹到资金费率上限)：(130-100)/100", "130", "131", "0.3"},
		{"买卖倒挂(正常撮合下不会出现)：正负两项互相抵消", "101", "99", "0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := premiumFromImpact(d("100"), d(c.bid), d(c.ask))
			if !got.Equal(d(c.want)) {
				t.Fatalf("premiumFromImpact(100, %s, %s) = %s, want %s", c.bid, c.ask, got, c.want)
			}
		})
	}
}
