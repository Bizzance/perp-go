package service

import (
	"context"
	"log"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/repo"
)

type InsuranceFundService struct {
	fund *repo.InsuranceFundRepo
}

func NewInsuranceFundService(fund *repo.InsuranceFundRepo) *InsuranceFundService {
	return &InsuranceFundService{fund: fund}
}

func (s *InsuranceFundService) FreshBalance(ctx context.Context) (decimal.Decimal, error) {
	return s.fund.FreshBalance(ctx)
}

// amount正数=强平盈余注入基金，负数=基金垫付穿仓亏损。基金余额允许为负(代表系统亏空)，MVP阶段只记日志告警，不做熔断
func (s *InsuranceFundService) Adjust(ctx context.Context, symbol string, uid, positionID uint64, amount decimal.Decimal, remark string) error {
	balanceAfter, err := s.fund.Adjust(ctx, symbol, uid, positionID, amount, remark, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	if balanceAfter.Sign() < 0 {
		log.Printf("[WARN] 保险基金余额穿仓！symbol=%s uid=%d amount=%s balanceAfter=%s remark=%s",
			symbol, uid, amount, balanceAfter, remark)
	}
	return nil
}
