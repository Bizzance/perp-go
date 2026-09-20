package indexfeed

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
)

// 喂价器的运行状态，给运维看、给监控告警用。只读快照，不含任何密钥
type Status struct {
	Healthy   bool           `json:"healthy"`
	StartedAt int64          `json:"startedAt"` // 毫秒
	Symbols   []SymbolStatus `json:"symbols"`
	Sources   []SourceStatus `json:"sources"`
}

type SymbolStatus struct {
	Symbol string `json:"symbol"`
	// 这个合约是不是健康：最近HealthMaxAge内成功发布过(还没发布过的话，从进程启动时间算起)
	Healthy       bool     `json:"healthy"`
	LastPrice     string   `json:"lastPrice"`     // 上次成功发布的价格，空=还没发布过
	LastPublishAt int64    `json:"lastPublishAt"` // 毫秒，0=还没发布过
	PublishAgeMs  int64    `json:"publishAgeMs"`  // 距离上次成功发布多久，-1=还没发布过
	LastSources   []string `json:"lastSources"`   // 上次发布用了哪些来源
	// 最近一次没发布成功的原因，成功发布后清空；FailStreak是连续多少个周期没发布成功
	Issue      string `json:"issue"`
	IssueAt    int64  `json:"issueAt"`
	FailStreak int    `json:"failStreak"`
}

type SourceStatus struct {
	Source      string `json:"source"`
	Symbol      string `json:"symbol"`
	OK          bool   `json:"ok"`        // 最近一次取价是否成功
	LastOKAt    int64  `json:"lastOkAt"`  // 毫秒，0=从没成功过
	LastError   string `json:"lastError"` // 最近一次失败的原因，成功后清空
	LastErrorAt int64  `json:"lastErrorAt"`
}

func ms(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// 当前状态的快照
func (f *Feeder) Status() Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	out := Status{Healthy: true, StartedAt: ms(f.startedAt)}
	for _, sym := range f.cfg.Symbols {
		st := f.state(sym)
		// 没成功发布过的，从进程启动时间开始算：刚启动的头几秒还没来得及发布，不算不健康
		since := st.lastPublishAt
		if since.IsZero() {
			since = f.startedAt
		}
		s := SymbolStatus{
			Symbol:        sym,
			Healthy:       now.Sub(since) <= f.cfg.HealthMaxAge,
			LastPublishAt: ms(st.lastPublishAt),
			PublishAgeMs:  -1,
			LastSources:   append([]string{}, st.lastSources...),
			Issue:         st.issue,
			IssueAt:       ms(st.issueAt),
			FailStreak:    st.failStreak,
		}
		if st.last.Sign() > 0 {
			s.LastPrice = st.last.String()
		}
		if !st.lastPublishAt.IsZero() {
			s.PublishAgeMs = now.Sub(st.lastPublishAt).Milliseconds()
		}
		if !s.Healthy {
			out.Healthy = false
		}
		out.Symbols = append(out.Symbols, s)
	}
	for key, ss := range f.sources {
		source, symbol, _ := strings.Cut(key, "/") // key的格式是"来源/合约"
		out.Sources = append(out.Sources, SourceStatus{
			Source: source, Symbol: symbol, OK: ss.ok,
			LastOKAt: ms(ss.lastOKAt), LastError: ss.err, LastErrorAt: ms(ss.errAt),
		})
	}
	sort.Slice(out.Sources, func(i, j int) bool {
		if out.Sources[i].Symbol != out.Sources[j].Symbol {
			return out.Sources[i].Symbol < out.Sources[j].Symbol
		}
		return out.Sources[i].Source < out.Sources[j].Source
	})
	return out
}

// 状态输出的HTTP入口：
//
//	GET /health  健康返回200，有合约超过HealthMaxAge没成功发布返回503(body是完整状态)。给容器健康检查和外部探活用
//	GET /status  始终返回200和完整状态的JSON。给人看、给监控抓取字段用
//
// 只读、不鉴权：里面是价格和来源的可用性，没有密钥，也不能通过它改变任何行为
func (f *Feeder) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		st := f.Status()
		code := http.StatusOK
		if !st.Healthy {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, st)
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, f.Status())
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
