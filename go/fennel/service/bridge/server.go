package main

import (
	"fennel/lib/sql"
	"fennel/mothership"
	actionC "fennel/mothership/controller/action"
	featureC "fennel/mothership/controller/feature"
	profileC "fennel/mothership/controller/profile"
	userC "fennel/mothership/controller/user"
	"fmt"
	"log"
	"net/http"
	"net/mail"
	"time"

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
	r.Use(WithFlashMessage)

	s := server{
		Engine:     r,
		mothership: m,
		args:       args,
	}
	s.setupRouter()

	return s, nil
}

const (
	SignInURL = "/signin"
)

func (s *server) setupRouter() {
	// Disable Console Color
	// gin.DisableConsoleColor()
	s.LoadHTMLGlob("templates/*.tmpl")
	s.Static("/images", "../../webapp/images")
	s.Static("/assets", "../../webapp/dist")

	s.GET("/signup", s.SignUpGet)
	s.POST("/signup", s.SignUp)
	s.GET(SignInURL, s.SignInGet)
	s.POST(SignInURL, s.SignIn)
	s.GET("/forgot_password", s.ForgotPasswordGet)
	s.POST("/forgot_password", s.ForgotPassword)
	s.GET("/confirm_user", s.ConfirmUser)
	s.POST("/resend_confirmation_email", s.ResendConfirmationEmail)

	auth := s.Group("/", AuthenticationRequired(s.mothership))
	auth.GET("/", s.Dashboard)
	auth.GET("/dashboard", s.Dashboard)
	auth.GET("/data", s.Data)
	auth.GET("/profiles", s.Profiles)
	auth.GET("/actions", s.Actions)
	auth.GET("/features", s.Features)
}

const (
	SignUpPage         = "signup"
	SignInPage         = "signin"
	ForgotPasswordPage = "forgot_password"
	DashboardPage      = "dashboard"
	DataPage           = "data"
)

func title(name string) string {
	return fmt.Sprintf("Fennel | %s", name)
}

func (s *server) SignUpGet(c *gin.Context) {
	c.HTML(http.StatusOK, "sign_on.tmpl", gin.H{
		"title": title("Sign Up"),
		"page":  SignUpPage,
	})
}

func (s *server) SignInGet(c *gin.Context) {
	c.HTML(http.StatusOK, "sign_on.tmpl", gin.H{
		"title":    title("Sign In"),
		"page":     SignInPage,
		"flashMsg": c.GetStringMapString(FlashMessageKey),
	})
}

func (s *server) ForgotPasswordGet(c *gin.Context) {
	c.HTML(http.StatusOK, "sign_on.tmpl", gin.H{
		"title": title("Forgot Password"),
		"page":  ForgotPasswordPage,
	})
}

func (s *server) ForgotPassword(c *gin.Context) {
	time.Sleep(time.Second)
	c.JSON(http.StatusBadRequest, gin.H{
		"error": "not implemented",
	})
}

func (s *server) ConfirmUser(c *gin.Context) {
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

	c.JSON(http.StatusCreated, gin.H{})
}

func (s *server) SignIn(c *gin.Context) {
	var form SignOnForm
	if err := c.BindJSON(&form); err != nil {
		// BindJSON would write status
		return
	}

	user, err := userC.SignIn(c.Request.Context(), s.mothership, form.Email, form.Password)

	if err != nil {
		// TODO(xiao) better error handling
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
	} else {
		saveUserIntoCookie(sessions.Default(c), user)
		c.JSON(http.StatusOK, gin.H{})
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
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Fail to resend confirmation email, please try again later.",
			})
			log.Printf("Failed to resend confirmation email: %v\n", err)
		}
	} else {
		c.JSON(http.StatusOK, gin.H{})
	}
}

func (s *server) Dashboard(c *gin.Context) {
	c.HTML(http.StatusOK, "index.tmpl", gin.H{"title": "Fennel | Dashboard", "page": DashboardPage})
}

func (s *server) Data(c *gin.Context) {
	c.HTML(http.StatusOK, "index.tmpl", gin.H{"title": "Fennel | Data", "page": DataPage})
}

func (s *server) Profiles(c *gin.Context) {
	var form struct {
		Otype string `form:"otype"`
		Oid   string `form:"oid"`

		sql.Pagination
	}
	if err := c.ShouldBind(&form); err != nil {
		log.Printf("Failed to parse Profiles params: %v\n", err)
		return
	}

	profiles, err := profileC.Profiles(c.Request.Context(), form.Otype, form.Oid, form.Pagination)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Fail to read profiles, please try again later.",
		})
		log.Printf("Failed to read profiles: %v\n", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"profiles": profiles,
	})
}

func (s *server) Actions(c *gin.Context) {
	var form struct {
		ActionType string `form:"action_type"`
		ActorType  string `form:"actor_type"`
		ActorID    string `form:"actor_id"`
		TargetType string `form:"target_type"`
		TargetID   string `form:"target_id"`
	}
	if err := c.ShouldBind(&form); err != nil {
		log.Printf("Failed to parse Actions params: %v\n", err)
		return
	}

	actions, err := actionC.Actions(c.Request.Context(), form.ActionType, form.ActorType, form.ActorID, form.TargetType, form.TargetID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Fail to read actions, please try again later.",
		})
		log.Printf("Failed to read actions: %v\n", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"actions": actions,
	})
}

func (s *server) Features(c *gin.Context) {
	features, err := featureC.Features(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Fail to read actions, please try again later.",
		})
		log.Printf("Failed to read actions: %v\n", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"features": features,
	})
}
