package main

import (
	"context"
	controller "fennel/controller/bridge"
	"fennel/mothership"
	"net/http"
	"net/mail"

	"github.com/alexflint/go-arg"
	"github.com/gin-gonic/gin"
)

type server struct {
	engine     *gin.Engine
	mothership mothership.Mothership
}

func NewServer() (server, error) {
	args := mothership.MothershipArgs{}
	err := arg.Parse(&args)
	if err != nil {
		return server{}, err
	}
	m, err := mothership.CreateFromArgs(&args)
	if err != nil {
		return server{}, err
	}
	r := gin.Default()
	s := server{
		engine:     r,
		mothership: m,
	}
	s.setupRouter()

	return s, nil
}

var db = make(map[string]string)

func (s *server) setupRouter() {
	// Disable Console Color
	// gin.DisableConsoleColor()
	s.engine.LoadHTMLGlob("templates/*.tmpl")
	s.engine.Static("/images", "../../webapp/images")
	s.engine.Static("/assets", "../../webapp/dist")

	// Ping test
	s.engine.GET("/ping", controller.Ping)

	s.engine.GET("/", controller.Dashboard)
	s.engine.GET("/dashboard", controller.Dashboard)
	s.engine.GET("/data", controller.Data)
	s.engine.GET("/profiles", controller.Profiles)

	s.engine.GET("/signup", s.SignUpGet)
	s.engine.POST("/signup", s.SignUp)
	s.engine.GET("/signin", s.SignInGet)
	s.engine.POST("/signin", s.SignIn)

	// (xiaoj) Example code below, to be deleted!!!

	// Authorized group (uses gin.BasicAuth() middleware)
	// Same than:
	// authorized := r.Group("/")
	// authorized.Use(gin.BasicAuth(gin.Credentials{
	//	  "foo":  "bar",
	//	  "manu": "123",
	//}))
	authorized := s.engine.Group("/", gin.BasicAuth(gin.Accounts{
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

func (s *server) Run(address string) error {
	return s.engine.Run(address)
}
