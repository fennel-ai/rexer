package main

import (
	"context"
	controller "fennel/controller/bridge"
	"fennel/mothership"
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

var db = make(map[string]string)

func (s *server) setupRouter() {
	// Disable Console Color
	// gin.DisableConsoleColor()
	s.LoadHTMLGlob("templates/*.tmpl")
	s.Static("/images", "../../webapp/images")
	s.Static("/assets", "../../webapp/dist")

	// Ping test
	s.GET("/ping", s.Ping)

	s.GET("/", controller.Dashboard)
	s.GET("/dashboard", controller.Dashboard)
	s.GET("/data", controller.Data)
	s.GET("/profiles", controller.Profiles)

	s.GET("/signup", s.SignUpGet)
	s.POST("/signup", s.SignUp)
	s.GET("/signin", s.SignInGet)
	s.POST("/signin", s.SignIn)

	// (xiaoj) Example code below, to be deleted!!!

	// Authorized group (uses gin.BasicAuth() middleware)
	// Same than:
	// authorized := r.Group("/")
	// authorized.Use(gin.BasicAuth(gin.Credentials{
	//	  "foo":  "bar",
	//	  "manu": "123",
	//}))
	authorized := s.Group("/", gin.BasicAuth(gin.Accounts{
		"foo":  "bar", // user:foo password:bar
		"manu": "123", // user:manu password:123
	}))

	/* example curl for /admin with basicauth header
	   Zm9vOmJhcg== is base64("foo:bar")

		curl -X POST \
	  	http://localhost:8080/admin \
	  	-H 'authorization: Basic Zm9vOmJhcg==' \
	  	-H 'content-type: application/json' \
	  	-d '{"value":"bar"}'
	*/
	authorized.POST("admin", func(c *gin.Context) {
		user := c.MustGet(gin.AuthUserKey).(string)

		// Parse JSON
		var json struct {
			Value string `json:"value" binding:"required"`
		}

		if c.Bind(&json) == nil {
			db[user] = json.Value
			c.JSON(http.StatusOK, gin.H{"status": "ok"})
		}
	})
}

func (s *server) Ping(c *gin.Context) {
	// TODO(xiao) remove testing code
	session := sessions.Default(c)
	token := session.Get("remember_token")
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

		session.Set("remember_token", user.RememberToken.String)
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
