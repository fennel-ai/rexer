package user

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fennel/mothership"
	"fmt"
	"time"

	"golang.org/x/crypto/bcrypt"

	lib "fennel/lib/user"
	"math/rand"
	"net/url"

	db "fennel/mothership/model/user"

	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
)

type ErrorUserNotFound struct{}

func (e *ErrorUserNotFound) Error() string {
	return "User not found"
}

type ErrorWrongPassword struct{}

func (e *ErrorWrongPassword) Error() string {
	return "Wrong password"
}

type ErrorNotConfirmed struct{}

func (e *ErrorNotConfirmed) Error() string {
	return "User not confirmed yet. Please confirm your email first."
}

type ErrorAlreadyConfirmed struct{}

func (e *ErrorAlreadyConfirmed) Error() string {
	return "User email is already confirmed"
}

func newUser(m mothership.Mothership, email, password string) (lib.User, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), 14)

	if err != nil {
		return lib.User{}, err
	}
	now := time.Now().UTC().UnixMicro()
	return lib.User{
		Email:             email,
		EncryptedPassword: hash,
		RememberToken:     sql.NullString{String: generateRememberToken(m), Valid: true},
		RememberCreatedAt: sql.NullInt64{Int64: now, Valid: true},
		CreatedAt:         now,
		UpdatedAt:         now,
	}, nil
}

func generateRememberToken(m mothership.Mothership) string {
	for {
		token := generateToken(m)
		if _, err := db.FetchByRememberToken(m, token); err != nil {
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
func SendConfirmationEmail(c context.Context, m mothership.Mothership, client *sendgrid.Client, user lib.User) (lib.User, error) {
	if user.IsConfirmed() {
		return user, &ErrorAlreadyConfirmed{}
	}
	token := generateConfirmationToken(m)
	user.ConfirmationToken = sql.NullString{
		String: token,
		Valid:  true,
	}
	var err error
	if user, err = db.UpdateConfirmation(m, user); err != nil {
		return user, err
	}

	from := mail.NewEmail("Xiao Jiang", "xiao+dev@fennel.ai")
	subject := "Welcome to Fennel.ai! Confirm Your Email"
	to := mail.NewEmail("", user.Email)

	link := generateConfirmationLink(token)
	plainTextContent := fmt.Sprintf("confirm email at %s", link.String())
	htmlContent := fmt.Sprintf(`confirm email at <a href="%s" target="_blank">...</a>`, link.String())
	message := mail.NewSingleEmail(from, subject, to, plainTextContent, htmlContent)
	if _, err := client.Send(message); err != nil {
		return user, err
	}
	user.ConfirmationSentAt = sql.NullInt64{
		Int64: time.Now().UTC().UnixMicro(),
		Valid: true,
	}
	return db.UpdateConfirmation(m, user)
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

func generateConfirmationToken(m mothership.Mothership) string {
	for {
		token := generateToken(m)
		if _, err := db.FetchByConfirmationToken(m, token); err != nil {
			return token
		}
	}
}

func generateResetToken(m mothership.Mothership) string {
	for {
		token := generateToken(m)
		if _, err := db.FetchByResetToken(m, token); err != nil {
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

func generateToken(m mothership.Mothership) string {
	bytes := make([]byte, 16)
	binary.LittleEndian.PutUint64(bytes, rand.Uint64())
	binary.LittleEndian.PutUint64(bytes[8:], rand.Uint64())
	return base64.RawURLEncoding.EncodeToString(bytes)
}

func ResendConfirmationEmail(c context.Context, m mothership.Mothership, client *sendgrid.Client, email string) error {
	user, err := db.FetchByEmail(m, email)
	if err != nil {
		return &ErrorUserNotFound{}
	}
	_, err = SendConfirmationEmail(c, m, client, user)
	return err
}

// TODO(xiao) add a test
// TODO(xiao) do not regenerate token if sent_at recent enough
func SendResetPasswordEmail(c context.Context, m mothership.Mothership, client *sendgrid.Client, email string) error {
	var err error
	user, err := db.FetchByEmail(m, email)
	if err != nil {
		return &ErrorUserNotFound{}
	}

	token := generateResetToken(m)
	user.ResetToken = sql.NullString{
		String: token,
		Valid:  true,
	}

	if user, err = db.UpdateResetInfo(m, user); err != nil {
		return err
	}

	from := mail.NewEmail("Xiao Jiang", "xiao+dev@fennel.ai")
	subject := "Link to Reset your password"
	to := mail.NewEmail("", user.Email)

	link := generateResetLink(token)
	plainTextContent := fmt.Sprintf("reset password at %s", link.String())
	htmlContent := fmt.Sprintf(`reset password at <a href="%s" target="_blank">...</a>`, link.String())
	message := mail.NewSingleEmail(from, subject, to, plainTextContent, htmlContent)
	if _, err := client.Send(message); err != nil {
		return err
	}
	user.ResetSentAt = sql.NullInt64{
		Int64: time.Now().UTC().UnixMicro(),
		Valid: true,
	}
	_, err = db.UpdateResetInfo(m, user)
	return err
}

func SignUp(c context.Context, m mothership.Mothership, email, password string) (lib.User, error) {
	_, err := db.FetchByEmail(m, email)
	if err == nil {
		return lib.User{}, errors.New("User already exists")
	}

	user, err := newUser(m, email, password)
	if err != nil {
		return user, err
	}
	uid, err := db.Insert(m, user)
	if err == nil {
		user.Id = uid
	}
	return user, err
}

func SignIn(c context.Context, m mothership.Mothership, email, password string) (lib.User, error) {
	user, err := db.FetchByEmail(m, email)
	if err != nil {
		return lib.User{}, &ErrorUserNotFound{}
	}

	if !checkPasswordHash(password, user.EncryptedPassword) {
		return lib.User{}, &ErrorWrongPassword{}
	}

	if !user.IsConfirmed() {
		return user, &ErrorNotConfirmed{}
	}
	return user, nil
}

func ConfirmUser(c context.Context, m mothership.Mothership, token string) (lib.User, error) {
	user, err := db.FetchByConfirmationToken(m, token)
	if err != nil {
		return lib.User{}, &ErrorUserNotFound{}
	}
	if user.IsConfirmed() {
		return user, nil
	}
	user.ConfirmedAt = sql.NullInt64{Int64: time.Now().UnixMicro(), Valid: true}
	user.ConfirmationToken = sql.NullString{Valid: false}
	user.ConfirmationSentAt = sql.NullInt64{Valid: false}
	return db.UpdateConfirmation(m, user)
}
