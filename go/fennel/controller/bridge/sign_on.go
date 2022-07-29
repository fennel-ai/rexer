package bridge

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fennel/mothership"
	"math/rand"
	"time"

	"golang.org/x/crypto/bcrypt"

	lib "fennel/lib/user"
	db "fennel/model/user"
)

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
	bytes := make([]byte, 16)
	for {
		binary.LittleEndian.PutUint64(bytes, rand.Uint64())
		binary.LittleEndian.PutUint64(bytes[8:], rand.Uint64())
		token := base64.RawURLEncoding.EncodeToString(bytes)
		if _, err := db.FetchByRememberToken(m, token); err != nil {
			return token
		}
	}
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

	user, err := newUser(m, email, password)
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
