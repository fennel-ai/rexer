package tier

import (
	"context"
	"errors"
	tierL "fennel/mothership/lib/tier"

	"gorm.io/gorm"
)

func FetchTiers(ctx context.Context, db *gorm.DB, customerID uint) (tiers []tierL.Tier, err error) {
	if customerID == 0 {
		return nil, errors.New("customer id is 0")
	}
	err = db.Joins("DataPlane").Find(&tiers, "customer_id = ?", customerID).Error

	return tiers, err
}
