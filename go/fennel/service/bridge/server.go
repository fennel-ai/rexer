package main

import (
	"fennel/lib/sql"
	"fennel/mothership"
	actionC "fennel/mothership/controller/action"
	featureC "fennel/mothership/controller/feature"
	profileC "fennel/mothership/controller/profile"
	userC "fennel/mothership/controller/user"
	"fennel/mothership/lib"
	"fennel/mothership/lib/customer"
	userL "fennel/mothership/lib/user"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
	"github.com/sendgrid/sendgrid-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type server struct {
	*gin.Engine
	mothership mothership.Mothership
	args       serverArgs
	db         *gorm.DB
}

type serverArgs struct {
	mothership.MothershipArgs
	SessionKey     string `arg:"required,--bridge_session_key,env:BRIDGE_SESSION_KEY"`
	SendgridAPIKey string `arg:"required,--sendgrid_api_key,env:SENDGRID_API_KEY"`
	BridgeENV      string `arg:"required,--bridge_env,env:BRIDGE_ENV"` // dev, prod
	AppPort        string `arg:"--app-port,env:APP_PORT" default:"8080"`

	RandSeed int64 `arg:"--rand_seed,env:RAND_SEED"`
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

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	if err != nil {
		return server{}, err
	}
	s := server{
		Engine:     r,
		mothership: m,
		args:       args,
		db:         db,
	}
	s.setupRouter()

	seed := args.RandSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rand.Seed(seed)
	log.Printf("Using rand seed %d\n", seed)

	return s, nil
}

const (
	SignInURL = "/signin"
)

func (s *server) setupRouter() {
	// Disable Console Color
	// gin.DisableConsoleColor()
	s.LoadHTMLGlob("mothership/templates/**/*.tmpl")
	s.Static("/images", "../../webapp/images")
	s.Static("/assets", "../../webapp/dist")

	s.GET("/signup", s.SignUpGet)
	s.POST("/signup", s.SignUp)
	s.GET(SignInURL, s.SignInGet)
	s.POST(SignInURL, s.SignIn)
	s.GET("/forgot_password", s.ForgotPasswordGet)
	s.POST("/forgot_password", s.ForgotPassword)
	s.GET("/reset_password", s.ResetPasswordGet)
	s.POST("/reset_password", s.ResetPassword)
	s.GET("/confirm_user", s.ConfirmUser)
	s.POST("/resend_confirmation_email", s.ResendConfirmationEmail)

	auth := s.Group("/", AuthenticationRequired(s.db))
	auth.GET("/", s.Dashboard)
	auth.GET("/dashboard", s.Dashboard)
	auth.GET("/data", s.Data)
	auth.GET("/profiles", s.Profiles)
	auth.GET("/actions", s.Actions)
	auth.GET("/features", s.Features)
	auth.GET("/settings", s.Settings)
	auth.POST("/logout", s.Logout)
	auth.GET("/user", s.User)
	auth.GET("/organization", s.Organization)
	auth.PATCH("/user_names", s.UpdateUserNames)
	auth.PATCH("/user_password", s.UpdateUserPassword)

	// dev only endpoints
	if s.isDev() {
		s.GET("/email/confirm", s.debugConfirmEmail)
		s.GET("/email/reset", s.debugResetPwdEmail)
	}
}

func (s *server) isDev() bool {
	return s.args.BridgeENV == "dev"
}

const (
	SignUpPage         = "signup"
	SignInPage         = "signin"
	ForgotPasswordPage = "forgot_password"
	ResetPasswordPage  = "reset_password"
	DashboardPage      = "dashboard"
	DataPage           = "data"
	SettingsPage       = "settings"
)

func title(name string) string {
	return fmt.Sprintf("Fennel | %s", name)
}

func (s *server) ResetPasswordGet(c *gin.Context) {
	c.HTML(http.StatusOK, "bridge/sign_on.tmpl", gin.H{
		"title": title("Reset Password"),
		"page":  ResetPasswordPage,
	})
}

func (s *server) SignUpGet(c *gin.Context) {
	c.HTML(http.StatusOK, "bridge/sign_on.tmpl", gin.H{
		"title": title("Sign Up"),
		"page":  SignUpPage,
	})
}

func (s *server) SignInGet(c *gin.Context) {
	c.HTML(http.StatusOK, "bridge/sign_on.tmpl", gin.H{
		"title":    title("Sign In"),
		"page":     SignInPage,
		"flashMsg": c.GetStringMapString(FlashMessageKey),
	})
}

func (s *server) ForgotPasswordGet(c *gin.Context) {
	c.HTML(http.StatusOK, "bridge/sign_on.tmpl", gin.H{
		"title": title("Forgot Password"),
		"page":  ForgotPasswordPage,
	})
}

func (s *server) ForgotPassword(c *gin.Context) {
	var form struct {
		Email string `json:"email"`
	}
	if err := c.BindJSON(&form); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	if err := userC.SendResetPasswordEmail(c.Request.Context(), s.db, s.sendgridClient(), form.Email); err != nil {
		respondError(c, err, "send the reset password email")
		return
	}

	c.JSON(http.StatusOK, gin.H{})
}

func (s *server) ResetPassword(c *gin.Context) {
	var form struct {
		Token    string `form:"token"`
		Password string `form:"password"`
	}
	if err := c.ShouldBind(&form); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	if err := userC.ResetPassword(c.Request.Context(), s.db, form.Token, form.Password); err != nil {
		respondError(c, err, "reset password")
		return
	}
	c.JSON(http.StatusOK, gin.H{})
}

func (s *server) ConfirmUser(c *gin.Context) {
	var form struct {
		Token string `form:"token"`
	}
	if err := c.ShouldBind(&form); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	session := sessions.Default(c)
	if _, err := userC.ConfirmUser(c.Request.Context(), s.db, form.Token); err != nil {
		msgDesc := ""
		if ue, ok := err.(*lib.UserReadableError); ok {
			msgDesc = ue.Msg
		} else {
			msgDesc = "Failed to confirm the email. Please try again later."
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
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	ctx := c.Request.Context()
	user, err := userC.SignUp(ctx, s.db, form.Email, form.Password)
	if err != nil {
		respondError(c, err, "sign up")
		return
	}
	if _, err = userC.SendConfirmationEmail(ctx, s.db, s.sendgridClient(), user); err != nil {
		respondError(c, err, "send confirmation email")
		return
	}

	c.JSON(http.StatusCreated, gin.H{})
}

func (s *server) SignIn(c *gin.Context) {
	var form SignOnForm
	if err := c.BindJSON(&form); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	user, err := userC.SignIn(c.Request.Context(), s.db, form.Email, form.Password)
	if err != nil {
		if ue, ok := err.(*lib.UserReadableError); ok {
			c.JSON(ue.StatusCode, gin.H{
				"error": ue.Msg,
			})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Failed to sign in. Please try again later.",
			})
			log.Printf("Failed to sign in: %v\n", err)
		}
		return
	}

	saveUserIntoCookie(sessions.Default(c), user)
	c.JSON(http.StatusOK, gin.H{})
}

func (s *server) ResendConfirmationEmail(c *gin.Context) {
	var form struct {
		Email string `json:"email"`
	}
	if err := c.BindJSON(&form); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	err := userC.ResendConfirmationEmail(c.Request.Context(), s.db, s.sendgridClient(), form.Email)
	if err != nil {
		if ue, ok := err.(*lib.UserReadableError); ok {
			c.JSON(ue.StatusCode, gin.H{
				"error": ue.Msg,
			})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Fail to resend confirmation email, please try again later.",
			})
			log.Printf("Failed to resend confirmation email: %v\n", err)
		}
		return
	}

	c.JSON(http.StatusOK, gin.H{})
}

func userMap(user userL.User) gin.H {
	// TODO(xiao) maybe add json tags on the user model
	return gin.H{
		"email":     user.Email,
		"firstName": user.FirstName,
		"lastName":  user.LastName,
	}
}

func (s *server) Dashboard(c *gin.Context) {
	user, _ := CurrentUser(c)
	c.HTML(http.StatusOK, "bridge/index.tmpl", gin.H{
		"title": title("Dashboard"),
		"page":  DashboardPage,
		"user":  userMap(user),
	})
}

func (s *server) Data(c *gin.Context) {
	user, _ := CurrentUser(c)
	c.HTML(http.StatusOK, "bridge/index.tmpl", gin.H{
		"title": title("Data"),
		"page":  DataPage,
		"user":  userMap(user),
	})
}

func (s *server) Settings(c *gin.Context) {
	user, _ := CurrentUser(c)
	c.HTML(http.StatusOK, "bridge/index.tmpl", gin.H{
		"title": title("Settings"),
		"page":  SettingsPage,
		"user":  userMap(user),
	})
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

func (s *server) Logout(c *gin.Context) {
	if user, ok := CurrentUser(c); ok {
		if _, err := userC.Logout(c.Request.Context(), s.db, user); err != nil {
			respondError(c, err, "log out the user")
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{})
}

func (s *server) User(c *gin.Context) {
	user, ok := CurrentUser(c)
	if !ok {
		// shouldn't happen, just in case of bug
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"error": "No user found",
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"user": userMap(user),
	})
}

func (s *server) Organization(c *gin.Context) {
	var customer customer.Customer
	user, ok := CurrentUser(c)
	ok = ok && (s.db.Take(&customer, user.CustomerID).RowsAffected > 0)

	var users []userL.User
	ok = ok && (s.db.Where("customer_id = ?", customer.ID).Find(&users).RowsAffected > 0)

	if !ok {
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"error": "No organization found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"organization": gin.H{
			"users": lo.Map(users, func(user userL.User, _ int) gin.H {
				return userMap(user)
			}),
		},
	})
}

func (s *server) UpdateUserNames(c *gin.Context) {
	var form struct {
		FirstName string `json:"firstName"`
		LastName  string `json:"lastName"`
	}
	if err := c.BindJSON(&form); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	if user, ok := CurrentUser(c); ok {
		if err := userC.UpdateUserNames(c.Request.Context(), s.db, user, form.FirstName, form.LastName); err != nil {
			respondError(c, err, "update user names")
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{})
}

func (s *server) UpdateUserPassword(c *gin.Context) {
	var form struct {
		CurrentPassword string `json:"currentPassword"`
		NewPassword     string `json:"newPassword"`
	}
	if err := c.BindJSON(&form); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	if user, ok := CurrentUser(c); ok {
		if _, err := userC.UpdatePassword(c.Request.Context(), s.db, user, form.CurrentPassword, form.NewPassword); err != nil {
			respondError(c, err, "update user password")
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{})
}

func respondError(c *gin.Context, err error, action string) {
	if ue, ok := err.(*lib.UserReadableError); ok {
		c.JSON(ue.StatusCode, gin.H{
			"error": ue.Msg,
		})
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to %s, please try again later.", action),
		})
		log.Printf("Failed to %s: %v\n", action, err)
	}
}

func (s *server) debugConfirmEmail(c *gin.Context) {
	c.HTML(http.StatusOK, "email/confirm_email.tmpl", gin.H{
		"ConfirmURL": "https://google.com",
		"Year":       2046,
	})
}

func (s *server) debugResetPwdEmail(c *gin.Context) {
	c.HTML(http.StatusOK, "email/reset_password.tmpl", gin.H{
		"ResetURL": "https://google.com",
		"Year":     2046,
	})
}
