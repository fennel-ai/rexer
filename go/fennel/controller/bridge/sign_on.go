package bridge

import (
	"context"
	"errors"
	"fennel/mothership"
	"time"

	"golang.org/x/crypto/bcrypt"

	lib "fennel/lib/user"
	db "fennel/model/user"
)

func NewUserFromForm(f Form) (lib.User, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(f.Password), 14)

	if err != nil {
		return lib.User{}, err
	}
	now := time.Now().UTC().UnixMicro()
	return lib.User{
		Email:             f.Email,
		EncryptedPassword: hash,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, nil
}

func checkPasswordHash(password string, hash []byte) bool {
	err := bcrypt.CompareHashAndPassword(hash, []byte(password))
	return err == nil
}

type Form struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func SignUp(c context.Context, m mothership.Mothership, form Form) (lib.User, error) {
	_, err := db.FetchByEmail(m, form.Email)
	if err == nil {
		return lib.User{}, errors.New("User already exists")
	}

	user, err := NewUserFromForm(form)
	if err != nil {
		return user, err
	}
	_, err = db.Insert(m, user)
	return user, err
}

func SignIn(c context.Context, m mothership.Mothership, form Form) (lib.User, error) {
	user, err := db.FetchByEmail(m, form.Email)

	if err != nil {
		return lib.User{}, errors.New("User not found")
	}
	if checkPasswordHash(form.Password, user.EncryptedPassword) {
		return user, nil
	} else {
		return lib.User{}, errors.New("Wrong password")
	}
}
