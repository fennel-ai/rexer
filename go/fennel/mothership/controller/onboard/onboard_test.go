package onboard

import (
	"context"
	"database/sql"
	"fennel/mothership"
	"fennel/mothership/lib/customer"
	customerL "fennel/mothership/lib/customer"
	tierL "fennel/mothership/lib/tier"
	userL "fennel/mothership/lib/user"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestTeamMatch(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	assert.Positive(t, db.Create(&customer.Customer{
		Name:   "Fennel",
		Domain: sql.NullString{String: "fennel.ai", Valid: true},
	}).RowsAffected)

	ctx := context.Background()

	user := userL.User{
		Email: "test@fennel.AI",
	}
	matched, customer, isPersonal := TeamMatch(ctx, db, user)
	assert.True(t, matched)
	assert.False(t, isPersonal)
	assert.Equal(t, "Fennel", customer.Name)

	user.Email = "test@fennel"
	matched, _, isPersonal = TeamMatch(ctx, db, user)
	assert.False(t, matched)
	assert.False(t, isPersonal)

	user.Email = "test@Gmail.com"
	matched, _, isPersonal = TeamMatch(ctx, db, user)
	assert.False(t, matched)
	assert.True(t, isPersonal)
}

func TestJoinTeam(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)
	ctx := context.Background()

	fennel := customer.Customer{
		Name:   "Fennel",
		Domain: sql.NullString{String: "fennel.ai", Valid: true},
	}
	assert.Positive(t, db.Create(&fennel).RowsAffected)

	google := customer.Customer{
		Name:   "Google",
		Domain: sql.NullString{Valid: false},
	}
	assert.Positive(t, db.Create(&google).RowsAffected)

	user := userL.User{
		Email:             "test@fennel.AI",
		EncryptedPassword: []byte("abcd"),
	}
	assert.Positive(t, db.Create(&user).RowsAffected)

	err = JoinTeam(ctx, db, fennel.ID, &user)
	assert.NoError(t, err)
	assert.Equal(t, userL.OnboardStatusTierProvisioning, user.OnboardStatus)

	err = JoinTeam(ctx, db, google.ID, &user)
	assert.Error(t, err)
}

func TestCreateTeam(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)
	ctx := context.Background()

	alice := userL.User{
		Email:             "alice@fennel.AI",
		EncryptedPassword: []byte("abcd"),
	}
	assert.Positive(t, db.Create(&alice).RowsAffected)

	customer, err := CreateTeam(ctx, db, "fennel", true, &alice)
	assert.NoError(t, err)
	assert.True(t, customer.Domain.Valid)
	assert.Equal(t, "fennel.ai", customer.Domain.String)
	assert.Equal(t, userL.OnboardStatusTierProvisioning, alice.OnboardStatus)

	bob := userL.User{
		Email:             "bob@fennel.ai",
		EncryptedPassword: []byte("abcd"),
	}
	assert.Positive(t, db.Create(&bob).RowsAffected)
	_, err = CreateTeam(ctx, db, "fennel", true, &bob)
	assert.Error(t, err)
	customer, err = CreateTeam(ctx, db, "fennel", false, &bob)
	assert.NoError(t, err)
	assert.False(t, customer.Domain.Valid)
	assert.Equal(t, userL.OnboardStatusTierProvisioning, bob.OnboardStatus)

	cat := userL.User{
		Email:             "cat@Hotmail.com",
		EncryptedPassword: []byte("abcd"),
	}
	assert.Positive(t, db.Create(&cat).RowsAffected)
	_, err = CreateTeam(ctx, db, "hot", true, &cat)
	assert.Error(t, err)
	customer, err = CreateTeam(ctx, db, "hot", false, &cat)
	assert.NoError(t, err)
	assert.False(t, customer.Domain.Valid)
	assert.Equal(t, userL.OnboardStatusTierProvisioning, cat.OnboardStatus)
}

func TestOnboardAssignTier(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)
	ctx := context.Background()

	user := userL.User{
		Email:             "test@fennel.ai",
		EncryptedPassword: []byte("abcd"),
	}
	assert.Positive(t, db.Create(&user).RowsAffected)

	_, _, err = AssignTier(ctx, db, &user)
	assert.ErrorContains(t, err, "user doesn't have a team")

	customer := customerL.Customer{
		Name: "fennel",
	}
	assert.Positive(t, db.Create(&customer).RowsAffected)
	user.CustomerID = customer.ID
	assert.Positive(t, db.Save(&user).RowsAffected)
	_, _, err = AssignTier(ctx, db, &user)
	assert.ErrorContains(t, err, "Unexpected onboard status")

	user.OnboardStatus = userL.OnboardStatusTierProvisioning
	_, available, err := AssignTier(ctx, db, &user)
	assert.NoError(t, err)
	assert.False(t, available)
	assert.Equal(t, userL.OnboardStatusTierProvisioning, user.OnboardStatus)

	user.OnboardStatus = userL.OnboardStatusTierProvisioning
	assert.Positive(t, db.Save(&user).RowsAffected)
	tier := tierL.Tier{
		DataPlaneID:   1,
		CustomerID:    customer.ID,
		PulumiStack:   "pulumi",
		ApiUrl:        "url",
		K8sNamespace:  "namespace",
		RequestsLimit: 100,
	}
	assert.Positive(t, db.Create(&tier).RowsAffected)

	assignedTier, available, err := AssignTier(ctx, db, &user)
	assert.NoError(t, err)
	assert.True(t, available)
	assert.Equal(t, tier.ID, assignedTier.ID)
	assert.Equal(t, userL.OnboardStatusTierProvisioned, user.OnboardStatus)

	user.OnboardStatus = userL.OnboardStatusTierProvisioning
	assert.Positive(t, db.Save(&user).RowsAffected)
	assignedTier, available, err = AssignTier(ctx, db, &user)
	assert.NoError(t, err)
	assert.True(t, available)
	assert.Equal(t, tier.ID, assignedTier.ID)
	assert.Equal(t, userL.OnboardStatusTierProvisioned, user.OnboardStatus)
}
