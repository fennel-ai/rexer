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

func newUser(email, password string) (lib.User, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), 14)

	if err != nil {
		return lib.User{}, err
	}
	now := time.Now().UTC().UnixMicro()
	return lib.User{
		Email:             email,
		EncryptedPassword: hash,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, nil
}

func checkPasswordHash(password string, hash []byte) bool {
	err := bcrypt.CompareHashAndPassword(hash, []byte(password))
	return err == nil
}

func SignUp(c context.Context, m mothership.Mothership, email, password string) (lib.User, error) {
	_, err := db.FetchByEmail(m, email)
	if err == nil {
		return lib.User{}, errors.New("User already exists")
	}

	user, err := newUser(email, password)
	if err != nil {
		return user, err
	}
	_, err = db.Insert(m, user)
	return user, err
}

func SignIn(c context.Context, m mothership.Mothership, email, password string) (lib.User, error) {
	user, err := db.FetchByEmail(m, email)

	if err != nil {
		return lib.User{}, errors.New("User not found")
	}
	if checkPasswordHash(password, user.EncryptedPassword) {
		return user, nil
	} else {
		return lib.User{}, errors.New("Wrong password")
	}
}
