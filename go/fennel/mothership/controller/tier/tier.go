package tier

import (
	"context"
	tierL "fennel/mothership/lib/tier"

	"gorm.io/gorm"
)

func FetchTiers(ctx context.Context, db *gorm.DB, customerID uint) (tiers []tierL.Tier, err error) {
	err = db.Joins("DataPlane").Find(&tiers, "customer_id = ?", customerID).Error

	return tiers, err
}
