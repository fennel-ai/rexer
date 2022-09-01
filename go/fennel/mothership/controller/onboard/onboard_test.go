package onboard

import (
	"context"
	"database/sql"
	"fennel/mothership"
	"fennel/mothership/lib/customer"
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

	nextStatus, err := JoinTeam(ctx, db, fennel.ID, user)
	assert.NoError(t, err)
	assert.Equal(t, userL.OnboardStatusAboutYourself, nextStatus)

	_, err = JoinTeam(ctx, db, google.ID, user)
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

	customer, nextStatus, err := CreateTeam(ctx, db, "fennel", true, alice)
	assert.NoError(t, err)
	assert.True(t, customer.Domain.Valid)
	assert.Equal(t, "fennel.ai", customer.Domain.String)
	assert.Equal(t, userL.OnboardStatusAboutYourself, nextStatus)

	bob := userL.User{
		Email:             "bob@fennel.ai",
		EncryptedPassword: []byte("abcd"),
	}
	assert.Positive(t, db.Create(&bob).RowsAffected)
	_, _, err = CreateTeam(ctx, db, "fennel", true, bob)
	assert.Error(t, err)
	customer, nextStatus, err = CreateTeam(ctx, db, "fennel", false, bob)
	assert.NoError(t, err)
	assert.False(t, customer.Domain.Valid)
	assert.Equal(t, userL.OnboardStatusAboutYourself, nextStatus)

	cat := userL.User{
		Email:             "cat@Hotmail.com",
		EncryptedPassword: []byte("abcd"),
	}
	assert.Positive(t, db.Create(&cat).RowsAffected)
	_, _, err = CreateTeam(ctx, db, "hot", true, cat)
	assert.Error(t, err)
	customer, nextStatus, err = CreateTeam(ctx, db, "hot", false, cat)
	assert.NoError(t, err)
	assert.False(t, customer.Domain.Valid)
	assert.Equal(t, userL.OnboardStatusAboutYourself, nextStatus)
}
