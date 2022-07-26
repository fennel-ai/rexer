package bridge

import (
	"net/http"
	"net/mail"
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
	Email    string `json:"email"`
	Password string `json:"-"`
}

func NewUserFromForm(f *Form) (*User, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(f.Password), 14)

	if err != nil {
		return nil, err
	}
	return &User{
		Email:    f.Email,
		Password: string(hash),
	}, nil
}

var users = make(map[string]User)

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
	if err := c.BindJSON(&form); err != nil {
		// BindJSON would write status
		return
	}

	if _, err := mail.ParseAddress(form.Email); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Bad email address",
		})
		return
	}

	if _, prs := users[form.Email]; prs {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "User already exists",
		})
		return
	}

	user, err := NewUserFromForm(&form)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Illegal password",
		})
		return
	}
	users[user.Email] = *user
	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"user": user,
		},
	})
}

func SignIn(c *gin.Context) {
	time.Sleep(time.Second) // TODO: remove

	var form Form
	if err := c.BindJSON(&form); err != nil {
		// BindJSON would write status
		return
	}

	user, prs := users[form.Email]

	if !prs {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "User not found",
		})
		return
	}
	if checkPasswordHash(form.Password, user.Password) {
		c.JSON(http.StatusOK, gin.H{
			"data": gin.H{
				"user": user,
			},
		})
	} else {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Wrong password",
		})
	}
}
