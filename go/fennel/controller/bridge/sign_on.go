package bridge

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

type SignUpForm struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type User struct {
	Email    string
	Password string
}

var users = make(map[string]User)

func hashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 14)
	return string(bytes), err
}

func checkPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

func SignUp(c *gin.Context) {
	var form SignUpForm
	_ = c.BindJSON(&form)

	if _, exists := users[form.Email]; exists || form.Email == "" {
		c.JSON(http.StatusOK, gin.H{
			"result": "user already exists or empty",
		})
	} else {
		hash, _ := hashPassword(form.Password)
		user := User{
			Email:    form.Email,
			Password: hash,
		}
		users[user.Email] = user
		c.JSON(http.StatusOK, gin.H{
			"email":    user.Email,
			"password": user.Password,
		})
	}
}

func SignIn(c *gin.Context) {
	var form SignUpForm
	_ = c.BindJSON(&form)
	if user, ok := users[form.Email]; ok && checkPasswordHash(form.Password, user.Password) {
		c.JSON(http.StatusOK, gin.H{
			"result": "found user",
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"result": "user not found",
		})
	}
}
