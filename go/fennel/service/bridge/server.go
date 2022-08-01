package main

import (
	"context"
	controller "fennel/controller/bridge"
	"fennel/model/user"
	"fennel/mothership"
	"log"
	"net/http"
	"net/mail"

	"github.com/alexflint/go-arg"
	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
)

type server struct {
	*gin.Engine
	mothership mothership.Mothership
}

type serverArgs struct {
	mothership.MothershipArgs
	SessionKey string `arg:"--bridge_session_key,env:BRIDGE_SESSION_KEY"`
}

func NewServer() (server, error) {
	args := serverArgs{}
	err := arg.Parse(&args)
	if err != nil {
		return server{}, err
	}
	m, err := mothership.CreateFromArgs(&args.MothershipArgs)
	if err != nil {
		return server{}, err
	}
	r := gin.Default()
	store := cookie.NewStore([]byte(args.SessionKey))
	r.Use(sessions.Sessions("mysession", store))
	s := server{
		Engine:     r,
		mothership: m,
	}
	s.setupRouter()

	return s, nil
}

const RememberTokenKey = "remember_token"
const CurrentUserKey = "current_user"

func (s *server) authenticationRequired() gin.HandlerFunc {
	log.Printf("aaaaaa!\n")
	return func(c *gin.Context) {
		log.Printf("triggred!\n")
		session := sessions.Default(c)
		token, ok := session.Get(RememberTokenKey).(string)
		if ok && token != "" {
			if user, err := user.FetchByRememberToken(s.mothership, token); err == nil {
				c.Set(CurrentUserKey, user)
				return
			}
		}
		log.Printf("no user found!\n")

		c.AbortWithStatus(http.StatusUnauthorized) // change to redirect
	}
}

func (s *server) setupRouter() {
	// Disable Console Color
	// gin.DisableConsoleColor()
	s.LoadHTMLGlob("templates/*.tmpl")
	s.Static("/images", "../../webapp/images")
	s.Static("/assets", "../../webapp/dist")

	// Ping test
	s.GET("/ping", s.Ping)

	auth := s.Group("/", s.authenticationRequired())

	auth.GET("/", controller.Dashboard)
	auth.GET("/dashboard", controller.Dashboard)
	auth.GET("/data", controller.Data)
	auth.GET("/profiles", controller.Profiles)

	s.GET("/signup", s.SignUpGet)
	s.POST("/signup", s.SignUp)
	s.GET("/signin", s.SignInGet)
	s.POST("/signin", s.SignIn)
}

func (s *server) Ping(c *gin.Context) {
	// TODO(xiao) remove testing code
	session := sessions.Default(c)
	token := session.Get(RememberTokenKey)
	c.JSON(http.StatusOK, gin.H{
		"ping":  "pong",
		"token": token,
	})
}

const (
	SignUpPage = "signup"
	SignInPage = "signin"
)

func (s *server) SignUpGet(c *gin.Context) {
	c.HTML(http.StatusOK, "sign_on.tmpl", gin.H{"title": "Fennel | SignUp", "page": SignUpPage})
}

func (s *server) SignInGet(c *gin.Context) {
	c.HTML(http.StatusOK, "sign_on.tmpl", gin.H{"title": "Fennel | SignIn", "page": SignInPage})
}

type SignOnForm struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (s *server) SignUp(c *gin.Context) {
	// time.Sleep(time.Second)

	var form SignOnForm
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

	ctx := context.Background()
	user, err := controller.SignUp(ctx, s.mothership, form.Email, form.Password)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
	} else {
		session := sessions.Default(c)

		session.Set(RememberTokenKey, user.RememberToken.String)
		_ = session.Save()
		c.JSON(http.StatusCreated, gin.H{
			"data": gin.H{
				"user": user,
			},
		})
	}
}

func (s *server) SignIn(c *gin.Context) {
	// time.Sleep(time.Second)

	var form SignOnForm
	if err := c.BindJSON(&form); err != nil {
		// BindJSON would write status
		return
	}

	ctx := context.Background()
	user, err := controller.SignIn(ctx, s.mothership, form.Email, form.Password)

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"data": gin.H{
				"user": user,
			},
		})
	}
}
