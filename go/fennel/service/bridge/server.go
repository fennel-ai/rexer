package main

import (
	"errors"
	"fennel/lib/sql"
	"fennel/mothership"
	actionC "fennel/mothership/controller/action"
	featureC "fennel/mothership/controller/feature"
	metricC "fennel/mothership/controller/metric"
	onboardC "fennel/mothership/controller/onboard"
	profileC "fennel/mothership/controller/profile"
	tierC "fennel/mothership/controller/tier"
	userC "fennel/mothership/controller/user"
	"fennel/mothership/lib"
	customerL "fennel/mothership/lib/customer"

	dataplaneL "fennel/mothership/lib/dataplane"
	tierL "fennel/mothership/lib/tier"
	"fennel/service/common"
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
	common.HealthCheckArgs
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
	common.StartHealthCheckServer(args.HealthPort)

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
	auth.GET("/onboard", s.Onboard)

	onboarded := auth.Group("/", Onboarded(s.db))
	onboarded.GET("/", s.TierManagement)
	onboarded.GET("/tier_management", s.TierManagement)
	onboarded.GET("/settings", s.Settings)

	tier := onboarded.Group("/tier/:id", TierPermission(s.db))
	tier.GET("/", s.Dashboard)
	tier.GET("/dashboard", s.Dashboard)
	tier.GET("/data", s.Data)
	tier.GET("/profiles", s.Profiles)
	tier.GET("/actions", s.Actions)
	tier.GET("/features", s.Features)

	// TODO(xiao) group under tier
	metrics := auth.Group("/metrics")
	metrics.GET("/query_range", s.QueryRangeMetrics)

	// ajax endpoints
	auth.GET("/tiers", s.Tiers)
	auth.POST("/logout", s.Logout)
	auth.GET("/user", s.User)
	auth.GET("/team", s.Team)
	auth.PATCH("/user_names", s.UpdateUserNames)
	auth.PATCH("/user_password", s.UpdateUserPassword)

	// onboard endpoints
	onboard := auth.Group("/onboard")
	onboard.GET("/team_match", s.OnboardTeamMatch)
	onboard.POST("/create_team", s.OnboardCreateTeam)
	onboard.POST("/join_team", s.OnboardJoinTeam)

	onboard.POST("/assign_tier", s.OnboardAssignTier)
	onboard.GET("/tier", s.OnboardTier)
	onboard.POST("/tier_provisioned", s.OnboardTierProvisioned)

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
	if err := userC.SendResetPasswordEmail(c.Request.Context(), s.db, s.sendgridClient(), form.Email, &s.mothership); err != nil {
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

func (s *server) sendgridClient() *sendgrid.Client {
	return sendgrid.NewSendClient(s.args.SendgridAPIKey)
}

func (s *server) SignUp(c *gin.Context) {
	var form struct {
		FirstName string `json:"firstName"`
		LastName  string `json:"lastName"`
		Email     string `json:"email"`
		Password  string `json:"password"`
	}

	if err := c.BindJSON(&form); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	ctx := c.Request.Context()
	user, err := userC.SignUp(ctx, s.db, form.FirstName, form.LastName, form.Email, form.Password)
	if err != nil {
		respondError(c, err, "sign up")
		return
	}
	if _, err = userC.SendConfirmationEmail(ctx, s.db, s.sendgridClient(), user, &s.mothership); err != nil {
		respondError(c, err, "send confirmation email")
		return
	}

	c.JSON(http.StatusCreated, gin.H{})
}

func (s *server) SignIn(c *gin.Context) {
	var form struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

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

	err := userC.ResendConfirmationEmail(c.Request.Context(), s.db, s.sendgridClient(), form.Email, &s.mothership)
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

func (s *server) Onboard(c *gin.Context) {
	c.HTML(http.StatusOK, "bridge/index.tmpl", bootstrapData(c, s.db, "Onboard"))
}

func (s *server) Dashboard(c *gin.Context) {
	c.HTML(http.StatusOK, "bridge/index.tmpl", bootstrapData(c, s.db, "Dashboard"))
}

func bootstrapData(c *gin.Context, db *gorm.DB, page string) gin.H {
	user, _ := CurrentUser(c)

	return gin.H{
		"title": title(page),
		"user":  userMap(user),
		"tiers": customerTiers(db, user.CustomerID),
	}
}

func (s *server) Data(c *gin.Context) {
	c.HTML(http.StatusOK, "bridge/index.tmpl", bootstrapData(c, s.db, "Data"))
}

func (s *server) Settings(c *gin.Context) {
	c.HTML(http.StatusOK, "bridge/index.tmpl", bootstrapData(c, s.db, "Settings"))
}

func (s *server) TierManagement(c *gin.Context) {
	c.HTML(http.StatusOK, "bridge/index.tmpl", bootstrapData(c, s.db, "Tier Management"))
}

func (s *server) Tiers(c *gin.Context) {
	user, _ := CurrentUser(c)

	tiers, err := tierC.FetchTiers(c.Request.Context(), s.db, user.CustomerID)
	if err != nil {
		respondError(c, err, "fetch tiers")
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"tiers": lo.Map(tiers, func(tier tierL.Tier, _ int) gin.H {
			return tierInfo(tier, tier.DataPlane)
		}),
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

	tier, _ := CurrentTier(c)
	profiles, err := profileC.Profiles(c.Request.Context(), tier, form.Otype, form.Oid, form.Pagination)
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

	tier, _ := CurrentTier(c)
	actions, err := actionC.Actions(c.Request.Context(), tier, form.ActionType, form.ActorType, form.ActorID, form.TargetType, form.TargetID)
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
	tier, _ := CurrentTier(c)
	features, err := featureC.Features(c.Request.Context(), tier)
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
		"user": gin.H{
			"email":     user.Email,
			"firstName": user.FirstName,
			"lastName":  user.LastName,
		},
	})
}

func (s *server) Team(c *gin.Context) {
	var customer customerL.Customer
	user, _ := CurrentUser(c)
	result := s.db.Take(&customer, user.CustomerID)

	if result.RowsAffected == 0 {
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"error": "No team found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"team": teamMembers(s.db, customer),
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

func (s *server) OnboardTeamMatch(c *gin.Context) {
	user, _ := CurrentUser(c)
	matched, team, isPersonalDomain := onboardC.TeamMatch(c.Request.Context(), s.db, user)
	if matched {
		c.JSON(http.StatusOK, gin.H{
			"matched":          matched,
			"team":             teamMembers(s.db, team),
			"isPersonalDomain": isPersonalDomain,
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"matched":          matched,
			"isPersonalDomain": isPersonalDomain,
		})
	}
}

func (s *server) OnboardCreateTeam(c *gin.Context) {
	var form struct {
		Name          string `json:"name"`
		AllowAutoJoin bool   `json:"allowAutoJoin"`
	}
	if err := c.BindJSON(&form); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	user, _ := CurrentUser(c)
	_, err := onboardC.CreateTeam(c.Request.Context(), s.db, form.Name, form.AllowAutoJoin, &user)
	if err != nil {
		respondError(c, err, "create team (onboard)")
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"onboardStatus": user.OnboardStatus,
	})
}

func (s *server) OnboardJoinTeam(c *gin.Context) {
	var form struct {
		TeamID uint `json:"teamID"`
	}
	if err := c.BindJSON(&form); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	user, _ := CurrentUser(c)
	err := onboardC.JoinTeam(c.Request.Context(), s.db, form.TeamID, &user)
	if err != nil {
		respondError(c, err, "join team (onboard)")
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"onboardStatus": user.OnboardStatus,
	})
}

func (s *server) OnboardAssignTier(c *gin.Context) {
	user, _ := CurrentUser(c)
	tier, available, err := onboardC.AssignTier(c.Request.Context(), s.db, &user)
	if err == nil && !available {
		err = errors.New("no available pre-provisioned tier")
	}
	if err != nil {
		respondError(c, err, "assign a tier (onboard)")
		return
	}
	if available {
		var dp dataplaneL.DataPlane
		_ = s.db.Take(&dp, tier.DataPlaneID)
		c.JSON(http.StatusOK, gin.H{
			"onboardStatus": user.OnboardStatus,
			"tier":          tierInfo(tier, dp),
		})
	}
}

func (s *server) OnboardTier(c *gin.Context) {
	user, _ := CurrentUser(c)
	tier, err := onboardC.FetchTier(c.Request.Context(), s.db, user.CustomerID)
	if err != nil {
		respondError(c, err, "assign a tier (onboard)")
		return
	}
	var dp dataplaneL.DataPlane
	_ = s.db.Take(&dp, tier.DataPlaneID)
	c.JSON(http.StatusOK, gin.H{
		"tier": tierInfo(tier, dp),
	})
}

func (s *server) OnboardTierProvisioned(c *gin.Context) {
	user, _ := CurrentUser(c)
	if err := onboardC.TierProvisioned(c.Request.Context(), s.db, &user); err != nil {
		respondError(c, err, "assign a tier (onboard)")
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"onboardStatus": user.OnboardStatus,
	})
}

type queryRangeRequest struct {
	Query string `form:"query"`
	Start string `form:"start"`
	End   string `form:"end"`
	Step  string `form:"step"`
}

func (s *server) QueryRangeMetrics(c *gin.Context) {
	var req queryRangeRequest
	if err := c.ShouldBind(&req); err != nil {
		respondError(c, err, "parse query range metrics params")
		return
	}
	start, end, step, err := req.parseParams()
	if err != nil {
		respondError(c, err, "parse query range metrics params")
		return
	}

	result, err := metricC.QueryRange(c.Request.Context(), req.Query, start, end, step)
	if err != nil {
		respondError(c, err, "query range metrics")
		return
	}
	c.JSON(http.StatusOK, result)
}

func (req queryRangeRequest) parseParams() (time.Time, time.Time, time.Duration, error) {
	var start, end time.Time
	var step time.Duration
	start, err := time.Parse(time.RFC3339, req.Start)
	if err != nil {
		return start, end, step, err
	}
	end, err = time.Parse(time.RFC3339, req.End)
	if err != nil {
		return start, end, step, err
	}
	step, err = time.ParseDuration(req.Step)
	return start, end, step, err
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
	c.HTML(http.StatusOK, "email/standard.tmpl", gin.H{
		"MothershipEndpoint": s.mothership.Endpoint,
		"Subject":            "Almost there, let’s confirm your email",
		"Title":              "You’re on your way! Let’s confirm your email address. 💌",
		"Desc":               "By clicking on the following link, you are confirming your email address.",
		"CTAText":            "Confirm email",
		"CTALink":            "http://google.com",
		"Year":               time.Now().Year(),
	})
}

func (s *server) debugResetPwdEmail(c *gin.Context) {
	c.HTML(http.StatusOK, "email/standard.tmpl", gin.H{
		"MothershipEndpoint": "",
		"Subject":            "Link to Reset your password",
		"Title":              "Here’s the link to reset your password 🔑",
		"CTAText":            "Reset Password",
		"CTALink":            "http://google.com",
		"Year":               time.Now().Year(),
	})
}
