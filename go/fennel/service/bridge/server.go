package main

import (
	controller "fennel/controller/bridge"
	userC "fennel/controller/user"
	"fennel/mothership"
	"log"
	"net/http"
	"net/mail"

	"github.com/alexflint/go-arg"
	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	"github.com/sendgrid/sendgrid-go"
)

type server struct {
	*gin.Engine
	mothership mothership.Mothership
	args       serverArgs
}

type serverArgs struct {
	mothership.MothershipArgs
	SessionKey     string `arg:"required,--bridge_session_key,env:BRIDGE_SESSION_KEY"`
	SendgridAPIKey string `arg:"required,--sendgrid_api_key,env:SENDGRID_API_KEY"`
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
	r.Use(withFlashMessage)

	s := server{
		Engine:     r,
		mothership: m,
		args:       args,
	}
	s.setupRouter()

	return s, nil
}

const (
	RememberTokenKey = "remember_token"
	SignInURL        = "/signin"
)

func (s *server) setupRouter() {
	// Disable Console Color
	// gin.DisableConsoleColor()
	s.LoadHTMLGlob("templates/*.tmpl")
	s.Static("/images", "../../webapp/images")
	s.Static("/assets", "../../webapp/dist")

	// Ping test
	s.GET("/ping", s.Ping)
	s.GET("/signup", s.SignUpGet)
	s.POST("/signup", s.SignUp)
	s.GET(SignInURL, s.SignInGet)
	s.POST(SignInURL, s.SignIn)
	s.GET("/resetpassword", s.ResetPassword)
	s.GET("/confirm_user", s.ConfirmUser)
	s.POST("/resend_confirmation_email", s.ResendConfirmationEmail)

	auth := s.Group("/", s.authenticationRequired())
	auth.GET("/", controller.Dashboard)
	auth.GET("/dashboard", controller.Dashboard)
	auth.GET("/data", controller.Data)
	auth.GET("/profiles", controller.Profiles)
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
	SignUpPage        = "signup"
	SignInPage        = "signin"
	ResetPasswordPage = "resetpassword"
)

const (
	FlashTypeError   = "error"
	FlashTypeSuccess = "success"
)

func (s *server) SignUpGet(c *gin.Context) {
	c.HTML(http.StatusOK, "sign_on.tmpl", gin.H{"title": "Fennel | SignUp", "page": SignUpPage})
}

func (s *server) SignInGet(c *gin.Context) {
	c.HTML(http.StatusOK, "sign_on.tmpl", gin.H{"title": "Fennel | SignIn", "page": SignInPage})
}

func (s *server) ResetPassword(c *gin.Context) {
	c.HTML(http.StatusOK, "sign_on.tmpl", gin.H{"title": "Fennel | Reset Password", "page": ResetPasswordPage})
}

func (s *server) ConfirmUser(c *gin.Context) {
	// TODO(xiao) polish (rediret to sign in page with flash message)
	var form struct {
		Token string `form:"token"`
	}
	if err := c.ShouldBind(&form); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	session := sessions.Default(c)
	if _, err := userC.ConfirmUser(c.Request.Context(), s.mothership, form.Token); err != nil {
		msgDesc := ""
		switch err.(type) {
		case *userC.ErrorUserNotFound:
			msgDesc = err.Error()
		default:
			msgDesc = "Something went wrong. Please try again."
		}
		addFlashMessage(session, FlashTypeError, msgDesc)
		c.Redirect(http.StatusFound, SignInURL)
		return
	}
	addFlashMessage(session, FlashTypeSuccess, "Your email address has been confirmed! You can now sign in.")
	c.Redirect(http.StatusFound, SignInURL)
}

type SignOnForm struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (s *server) sendgridClient() *sendgrid.Client {
	return sendgrid.NewSendClient(s.args.SendgridAPIKey)
}

func (s *server) SignUp(c *gin.Context) {
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

	ctx := c.Request.Context()
	user, err := userC.SignUp(ctx, s.mothership, form.Email, form.Password)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	if _, err = userC.SendConfirmationEmail(ctx, s.mothership, s.sendgridClient(), user); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Failed to send confirmation email, please try again!",
		})
		log.Printf("Failed to send confirmation email: %v\n", err)
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"data": gin.H{},
	})
}

func (s *server) SignIn(c *gin.Context) {
	var form SignOnForm
	if err := c.BindJSON(&form); err != nil {
		// BindJSON would write status
		return
	}

	user, err := userC.SignIn(c.Request.Context(), s.mothership, form.Email, form.Password)

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
	} else {
		saveUserIntoCookie(sessions.Default(c), user)
		c.JSON(http.StatusOK, gin.H{
			"data": gin.H{},
		})
	}
}

func (s *server) ResendConfirmationEmail(c *gin.Context) {
	var form struct {
		Email string `json:"email"`
	}
	if err := c.BindJSON(&form); err != nil {
		// BindJSON would write status
		return
	}

	err := userC.ResendConfirmationEmail(c.Request.Context(), s.mothership, s.sendgridClient(), form.Email)
	if err != nil {
		switch err.(type) {
		case *userC.ErrorUserNotFound, *userC.ErrorAlreadyConfirmed:
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
		default:
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Fail to resend confirmation email, please try again later.",
			})
			log.Printf("Failed to resend confirmation email: %v\n", err)
		}
	} else {
		c.JSON(http.StatusOK, gin.H{
			"data": gin.H{},
		})
	}
}
