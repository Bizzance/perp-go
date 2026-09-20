package config

import (
	"testing"
	"time"
)

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

func TestParseAPIKeys(t *testing.T) {
	keys, err := ParseAPIKeys("partner-a:0123456789abcdef0123:trade|ops, feeder:abcdef0123456789abcd:ops")
	if err != nil || len(keys) != 2 {
		t.Fatalf("合法配置应该解析成功, got %v %v", keys, err)
	}
	if keys[0].ID != "partner-a" || len(keys[0].Scopes) != 2 || keys[1].Scopes[0] != "ops" {
		t.Errorf("解析结果不对: %+v", keys)
	}
	if keys, err := ParseAPIKeys("  "); err != nil || keys != nil {
		t.Errorf("空配置=没有密钥，不是错误, got %v %v", keys, err)
	}
	bad := map[string]string{
		"格式不对":     "onlyid",
		"缺权限":      "a:0123456789abcdef0123:",
		"权限非法":     "a:0123456789abcdef0123:admin",
		"secret太短": "a:short:trade",
		"id为空":     ":0123456789abcdef0123:trade",
		"id重复":     "a:0123456789abcdef0123:trade,a:abcdef0123456789abcd:ops",
	}
	for name, raw := range bad {
		if _, err := ParseAPIKeys(raw); err == nil {
			t.Errorf("%s: 应该报错: %q", name, raw)
		}
	}
}

func TestParseAPIKeys_RejectsTemplatePlaceholders(t *testing.T) {
	// 模板里的占位值就算补长到16位以上也必须拒绝，否则忘了改的部署会带着一个写在仓库里的密钥启动
	for _, raw := range []string{
		"partner-a:CHANGE_ME_SECRET:trade",
		"partner-a:change_me_change_me_change_me:trade",
		"partner-a:xxxxCHANGE_MExxxxxxxxxxxx:ops",
	} {
		if _, err := ParseAPIKeys(raw); err == nil {
			t.Errorf("占位密钥应该被拒绝: %q", raw)
		}
	}
	if _, err := ParseAPIKeys("partner-a:9f2c4e6a8b0d1f3a5c7e9b1d3f5a7c9e:trade"); err != nil {
		t.Errorf("随机密钥应该通过, got %v", err)
	}
}

func TestConfigValidateAuth(t *testing.T) {
	if err := (Config{}).ValidateAuth(); err == nil {
		t.Error("默认开启鉴权且没有密钥，必须拒绝启动")
	}
	if err := (Config{AuthDisabled: true}).ValidateAuth(); err != nil {
		t.Errorf("显式关闭鉴权(本地开发)应该允许, got %v", err)
	}
	if err := (Config{APIKeys: []APIKey{{ID: "a", Secret: "0123456789abcdef", Scopes: []string{"trade"}}}}).ValidateAuth(); err != nil {
		t.Errorf("有密钥应该通过, got %v", err)
	}
}

// 标记价的配置：没设用默认值；设了按设的值；PERP_MARK_REQUIRE_INDEX只有"true"才开
func TestLoad_MarkPriceSettings(t *testing.T) {
	t.Setenv("PERP_MARK_MAX_INDEX_AGE_SEC", "")
	t.Setenv("PERP_MARK_MAX_DEVIATION", "")
	t.Setenv("PERP_MARK_BASIS_WINDOW_SEC", "")
	t.Setenv("PERP_MARK_REQUIRE_INDEX", "")
	def := Load(0)
	if def.MarkPriceMaxIndexAge != 30*time.Second || def.MarkPriceMaxDeviation != 0.01 ||
		def.MarkPriceBasisWindow != 60*time.Second || def.MarkPriceRequireIndex || def.MarkPriceRefreshMs != 1000 {
		t.Fatalf("默认值不对: %+v", def)
	}

	t.Setenv("PERP_MARK_MAX_INDEX_AGE_SEC", "10")
	t.Setenv("PERP_MARK_MAX_DEVIATION", "0.005")
	t.Setenv("PERP_MARK_BASIS_WINDOW_SEC", "120")
	t.Setenv("PERP_MARK_REQUIRE_INDEX", "true")
	got := Load(0)
	if got.MarkPriceMaxIndexAge != 10*time.Second || got.MarkPriceMaxDeviation != 0.005 ||
		got.MarkPriceBasisWindow != 120*time.Second || !got.MarkPriceRequireIndex {
		t.Fatalf("设置的值没生效: %+v", got)
	}

	t.Setenv("PERP_MARK_REQUIRE_INDEX", "1")
	if Load(0).MarkPriceRequireIndex {
		t.Fatal(`只有"true"才开启RequireIndex，"1"不算`)
	}
}
