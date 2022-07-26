package bridge

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

const DashboardPage = "dashboard"
const DataPage = "data"
const SignUpPage = "signup"
const SignInPage = "signin"

type Form struct {
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

func SignUpGet(c *gin.Context) {
	c.HTML(http.StatusOK, "sign_on.tmpl", gin.H{"title": "Fennel | SignUp", "page": SignUpPage})
}

func SignInGet(c *gin.Context) {
	c.HTML(http.StatusOK, "sign_on.tmpl", gin.H{"title": "Fennel | SignIn", "page": SignInPage})
}

func SignUp(c *gin.Context) {
	time.Sleep(time.Second)

	var form Form
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
	time.Sleep(2 * time.Second)

	var form Form
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
