package user

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"html/template"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	lib "fennel/mothership/lib"
	userL "fennel/mothership/lib/user"
	"math/rand"
	netmail "net/mail"
	"net/url"

	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
)

const BCRYPT_COST = 14

func newUser(db *gorm.DB, email, password string) (userL.User, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), BCRYPT_COST)
	if err != nil {
		return userL.User{}, err
	}

	return userL.User{
		Email:             email,
		EncryptedPassword: hash,
		FirstName:         "Xiao", // TODO(xiao) pass params from upstream
		LastName:          "Jiang",
	}, nil
}

func generateRememberToken(db *gorm.DB) string {
	for {
		token := generateToken()
		result := db.Take(&userL.User{}, "remember_token = ?", token)
		if result.RowsAffected == 0 {
			return token
		}
	}
}

func checkPasswordHash(password string, hash []byte) bool {
	err := bcrypt.CompareHashAndPassword(hash, []byte(password))
	return err == nil
}

// TODO(xiao) add a unit test for email
// TODO(xiao) do not regen token if sent_at is recent enough
// TODO(xiao) do not return user
func SendConfirmationEmail(c context.Context, db *gorm.DB, client *sendgrid.Client, user userL.User) (userL.User, error) {
	if user.IsConfirmed() {
		return user, &lib.ErrorAlreadyConfirmed
	}
	token := generateConfirmationToken(db)
	result := db.Model(&user).Update("ConfirmationToken", token)

	if result.Error != nil {
		return user, result.Error
	}

	from := mail.NewEmail("Fennel AI", "xiao+dev@fennel.ai")
	subject := "Almost there, let’s confirm your email"
	to := mail.NewEmail("", user.Email)

	link := generateConfirmationLink(token)

	// TODO(xiao) plaintext fallback
	plainTextContent := ""

	tmpl, err := template.ParseFiles("mothership/templates/email/confirm_email.tmpl")
	if err != nil {
		return user, err
	}
	data := struct {
		ConfirmURL string
		Year       int
	}{
		ConfirmURL: link.String(),
		Year:       time.Now().Year(),
	}
	buf := bytes.NewBufferString("")

	if err := tmpl.ExecuteTemplate(buf, "email/confirm_email.tmpl", data); err != nil {
		return user, err
	}
	htmlContent := buf.String()
	message := mail.NewSingleEmail(from, subject, to, plainTextContent, htmlContent)
	if _, err := client.Send(message); err != nil {
		return user, err
	}

	result = db.Model(&user).Update("ConfirmationSentAt", time.Now().UTC().UnixMicro())
	return user, result.Error
}

func generateConfirmationLink(token string) url.URL {
	// TODO(xiao) read host from config
	return url.URL{
		Scheme:   "http",
		Host:     "localhost:8080",
		Path:     "confirm_user",
		RawQuery: fmt.Sprintf("token=%s", token),
	}
}

func generateConfirmationToken(db *gorm.DB) string {
	for {
		token := generateToken()
		result := db.Take(&userL.User{}, "confirmation_token = ?", token)
		if result.RowsAffected == 0 {
			return token
		}
	}
}

func generateResetToken(db *gorm.DB) string {
	for {
		token := generateToken()
		result := db.Take(&userL.User{}, "reset_token = ?", token)
		if result.RowsAffected == 0 {
			return token
		}
	}
}

func generateResetLink(token string) url.URL {
	// TODO(xiao) read host from config
	return url.URL{
		Scheme:   "http",
		Host:     "localhost:8080",
		Path:     "reset_password",
		RawQuery: fmt.Sprintf("token=%s", token),
	}
}

func generateToken() string {
	bytes := make([]byte, 16)
	binary.LittleEndian.PutUint64(bytes, rand.Uint64())
	binary.LittleEndian.PutUint64(bytes[8:], rand.Uint64())
	return base64.RawURLEncoding.EncodeToString(bytes)
}

func ResendConfirmationEmail(c context.Context, db *gorm.DB, client *sendgrid.Client, email string) error {
	var user userL.User
	result := db.Take(&user, "email = ?", email)
	if result.Error != nil {
		return &lib.ErrorUserNotFound
	}
	_, err := SendConfirmationEmail(c, db, client, user)
	return err
}

// TODO(xiao) add a test
// TODO(xiao) do not regenerate token if sent_at recent enough
// TODO(xiao) DRY the tmpl file
func SendResetPasswordEmail(c context.Context, db *gorm.DB, client *sendgrid.Client, email string) error {
	var user userL.User

	result := db.Take(&user, "email = ?", email)
	if result.Error != nil {
		return &lib.ErrorUserNotFound
	}

	token := generateResetToken(db)
	result = db.Model(&user).Update("ResetToken", token)

	if result.Error != nil {
		return result.Error
	}

	from := mail.NewEmail("Fennel AI", "xiao+dev@fennel.ai")
	subject := "Link to Reset your password"
	to := mail.NewEmail("", user.Email)

	link := generateResetLink(token)
	plainTextContent := ""

	tmpl, err := template.ParseFiles("mothership/templates/email/reset_password.tmpl")
	if err != nil {
		return err
	}
	data := struct {
		ResetURL string
		Year     int
	}{
		ResetURL: link.String(),
		Year:     time.Now().Year(),
	}
	buf := bytes.NewBufferString("")

	if err := tmpl.ExecuteTemplate(buf, "email/reset_password.tmpl", data); err != nil {
		return err
	}
	htmlContent := buf.String()

	message := mail.NewSingleEmail(from, subject, to, plainTextContent, htmlContent)
	if _, err := client.Send(message); err != nil {
		return err
	}

	result = db.Model(&user).Update("ResetSentAt", time.Now().UTC().UnixMicro())
	return result.Error
}

func SignUp(c context.Context, db *gorm.DB, email, password string) (userL.User, error) {
	var user userL.User

	if _, err := netmail.ParseAddress(email); err != nil {
		return user, &lib.ErrorBadEmail
	}

	result := db.Take(&user, "email = ?", email)
	if result.RowsAffected > 0 {
		return userL.User{}, &lib.ErrorUserAlreadySignedUp
	}

	user, err := newUser(db, email, password)
	if err != nil {
		return user, err
	}
	result = db.Create(&user)
	return user, result.Error
}

// TODO(xiao) do not generate remember token if already generated (recently)
func SignIn(c context.Context, db *gorm.DB, email, password string) (userL.User, error) {
	var user userL.User

	result := db.Take(&user, "email = ?", email)
	if result.Error != nil {
		return userL.User{}, &lib.ErrorUserNotFound
	}

	if !checkPasswordHash(password, user.EncryptedPassword) {
		return userL.User{}, &lib.ErrorWrongPassword
	}

	if !user.IsConfirmed() {
		return user, &lib.ErrorNotConfirmed
	}
	user.RememberCreatedAt = sql.NullInt64{Int64: time.Now().UTC().UnixMicro(), Valid: true}
	result = db.Model(&user).Updates(map[string]interface{}{
		"RememberToken":     generateRememberToken(db),
		"RememberCreatedAt": time.Now().UTC().UnixMicro(),
	})
	return user, result.Error
}

func ConfirmUser(c context.Context, db *gorm.DB, token string) (userL.User, error) {
	var user userL.User

	result := db.Take(&user, "confirmation_token = ?", token)
	if result.Error != nil {
		return userL.User{}, &lib.ErrorUserNotFound
	}
	if user.IsConfirmed() {
		return user, nil
	}
	user.ConfirmedAt = sql.NullInt64{Int64: time.Now().UnixMicro(), Valid: true}
	user.ConfirmationToken = sql.NullString{Valid: false}
	user.ConfirmationSentAt = sql.NullInt64{Valid: false}
	result = db.Model(&user).Updates(map[string]interface{}{
		"ConfirmedAt":        time.Now().UnixMicro(),
		"ConfirmationToken":  sql.NullString{Valid: false},
		"ConfirmationSentAt": sql.NullInt64{Valid: false},
	})

	return user, result.Error
}

func ResetPassword(c context.Context, db *gorm.DB, token, password string) error {
	var user userL.User

	result := db.Take(&user, "reset_token = ?", token)
	if result.Error != nil {
		return &lib.ErrorUserNotFound
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), BCRYPT_COST)
	if err != nil {
		return err
	}
	result = db.Model(&user).Updates(map[string]interface{}{
		"EncryptedPassword": hash,
		"ResetToken":        sql.NullString{Valid: false},
		"ResetSentAt":       sql.NullInt64{Valid: false},
	})
	return result.Error
}

func Logout(c context.Context, db *gorm.DB, user userL.User) (userL.User, error) {
	result := db.Model(&user).Updates(map[string]interface{}{
		"RememberToken":     sql.NullString{Valid: false},
		"RememberCreatedAt": sql.NullInt64{Valid: false},
	})
	return user, result.Error
}

func UpdateUserNames(c context.Context, db *gorm.DB, user userL.User, firstName, lastName string) error {
	user.FirstName = firstName
	user.LastName = lastName
	return db.Save(&user).Error
}

func UpdatePassword(c context.Context, db *gorm.DB, user userL.User, currentPassword, newPassword string) (userL.User, error) {
	if !checkPasswordHash(currentPassword, user.EncryptedPassword) {
		return user, &lib.ErrorWrongPassword
	}
	newHash, err := bcrypt.GenerateFromPassword([]byte(newPassword), BCRYPT_COST)
	if err != nil {
		return user, err
	}
	user.EncryptedPassword = newHash
	err = db.Save(&user).Error
	return user, err
}
