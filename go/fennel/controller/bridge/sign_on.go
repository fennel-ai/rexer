package bridge

import (
	"context"
	"errors"

	"golang.org/x/crypto/bcrypt"
)

const DashboardPage = "dashboard"
const DataPage = "data"

type User struct {
	Email    string `json:"email"`
	Password string `json:"-"`
}

func NewUserFromForm(f Form) (User, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(f.Password), 14)

	if err != nil {
		return User{}, err
	}
	return User{
		Email:    f.Email,
		Password: string(hash),
	}, nil
}

var users = make(map[string]User)

func checkPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

type Form struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func SignUp(c context.Context, form Form) (User, error) {
	if _, prs := users[form.Email]; prs {
		return User{}, errors.New("User already exists")
	}

	user, err := NewUserFromForm(form)
	if err == nil {
		users[user.Email] = user
	}
	return user, nil
}

func SignIn(c context.Context, form Form) (User, error) {
	user, prs := users[form.Email]

	if !prs {
		return User{}, errors.New("User not found")
	}
	if checkPasswordHash(form.Password, user.Password) {
		return user, nil
	} else {
		return User{}, errors.New("Wrong password")
	}
}
