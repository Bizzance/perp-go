package indexfeed

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/api"
)

// 用API Key签名后调contract-api的POST /index-price，需要ops权限的密钥。
// 签名用的是生产代码里的api.SignRequest，跟合作方对接用的是同一套
type APIPublisher struct {
	BaseURL string // 形如http://127.0.0.1:7001，不带结尾的斜杠
	KeyID   string
	Secret  string
	Client  *http.Client
}

const indexPricePath = "/index-price"

// contract-api的服务端跳变保护拦下了这次推送(errCode=index_price_jump)：新价位还在等确认，
// 不是接口故障，也不是签名/权限问题。喂价器按周期继续推，服务端满了确认时间就会承认
var ErrJumpGuard = errors.New("服务端跳变保护")

func (p *APIPublisher) Publish(ctx context.Context, symbol string, price decimal.Decimal) error {
	body, err := json.Marshal(map[string]string{"symbol": symbol, "price": price.String()})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.BaseURL, "/")+indexPricePath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	nb := make([]byte, 12)
	_, _ = rand.Read(nb)
	nonce := hex.EncodeToString(nb)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", p.KeyID)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", api.SignRequest(p.Secret, ts, nonce, http.MethodPost, indexPricePath, "", body))

	resp, err := p.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return err
	}
	// 业务错误也是HTTP 200，错误在响应体的code里
	var env struct {
		Code    int    `json:"code"`
		ErrCode string `json:"errCode"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("HTTP %d, 响应不是JSON: %s", resp.StatusCode, truncate(string(raw), 200))
	}
	if env.ErrCode == api.ErrIndexPriceJump {
		return fmt.Errorf("%w: %s", ErrJumpGuard, env.Message)
	}
	if env.Code != 200 {
		return fmt.Errorf("HTTP %d code=%d errCode=%s message=%s", resp.StatusCode, env.Code, env.ErrCode, env.Message)
	}
	return nil
}
