// 保证金分档(风险限额)配置——见sql/schema.sql的risk_limit_tiers表头注释
package repo

import (
	"context"

	"github.com/jmoiron/sqlx"

	"perp-go/internal/model"
)

type RiskLimitRepo struct{ db *sqlx.DB }

func NewRiskLimitRepo(db *sqlx.DB) *RiskLimitRepo { return &RiskLimitRepo{db: db} }

// FindBySymbol 按tier升序返回这个symbol配置的全部档位——PositionService.TierFor按顺序
// 找第一个覆盖到目标名义价值的档位，靠的就是这个顺序
func (r *RiskLimitRepo) FindBySymbol(ctx context.Context, symbol string) ([]model.RiskLimitTier, error) {
	var tiers []model.RiskLimitTier
	err := r.db.SelectContext(ctx, &tiers, `SELECT * FROM risk_limit_tiers WHERE symbol = ? ORDER BY tier ASC`, symbol)
	return tiers, err
}
