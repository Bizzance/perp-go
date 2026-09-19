package config

import "testing"

func TestParseEngineSymbols(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"空字符串=没分片", "", nil},
		{"单个symbol", "BTCUSDT", []string{"BTCUSDT"}},
		{"多个symbol逗号分隔", "BTCUSDT,ETHUSDT", []string{"BTCUSDT", "ETHUSDT"}},
		{"容忍多余空格", " BTCUSDT , ETHUSDT ", []string{"BTCUSDT", "ETHUSDT"}},
		{"容忍多余逗号(空segment跳过)", "BTCUSDT,,ETHUSDT,", []string{"BTCUSDT", "ETHUSDT"}},
		{"全是空白/逗号=等同于没配置", " , , ", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseEngineSymbols(c.in)
			if len(got) != len(c.want) {
				t.Fatalf("输入%q: 期望%v, 实际%v", c.in, c.want, got)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("输入%q: 期望%v, 实际%v", c.in, c.want, got)
				}
			}
		})
	}
}

// 验证NodeIDExplicit只在真的设了PERP_NODE_ID环境变量时才为true——
// engine分片模式下main.go靠这个字段做启动时的fail-fast校验(见docs/engine-sharding.md)，
// 如果这个字段被错误地标记成true，那道校验就会形同虚设
func TestLoad_NodeIDExplicit(t *testing.T) {
	t.Setenv("PERP_NODE_ID", "")
	cfg := Load(7)
	if cfg.NodeIDExplicit {
		t.Fatalf("没设PERP_NODE_ID时NodeIDExplicit应该是false")
	}
	if cfg.NodeID != 7 {
		t.Fatalf("没设PERP_NODE_ID时应该用传入的默认值7, 实际%d", cfg.NodeID)
	}

	t.Setenv("PERP_NODE_ID", "42")
	cfg = Load(7)
	if !cfg.NodeIDExplicit {
		t.Fatalf("设了PERP_NODE_ID时NodeIDExplicit应该是true")
	}
	if cfg.NodeID != 42 {
		t.Fatalf("设了PERP_NODE_ID=42时应该用42, 实际%d", cfg.NodeID)
	}
}
