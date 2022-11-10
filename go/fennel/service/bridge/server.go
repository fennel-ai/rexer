package main

import (
	"encoding/json"
	"errors"
	actionL "fennel/lib/action"
	profileL "fennel/lib/profile"
	"fennel/lib/sql"
	"fennel/mothership"
	actionC "fennel/mothership/controller/action"
	featureC "fennel/mothership/controller/feature"
	metricC "fennel/mothership/controller/metric"
	onboardC "fennel/mothership/controller/onboard"
	profileC "fennel/mothership/controller/profile"
	queryC "fennel/mothership/controller/query"
	tierC "fennel/mothership/controller/tier"
	userC "fennel/mothership/controller/user"
	"fennel/mothership/lib"
	customerL "fennel/mothership/lib/customer"
	dataplaneL "fennel/mothership/lib/dataplane"
	ginL "fennel/mothership/lib/gin"
	serializerL "fennel/mothership/lib/serializer"
	tierL "fennel/mothership/lib/tier"
	"fennel/service/common"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
	"github.com/sendgrid/sendgrid-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	SignInURL         = "/signin"
	TierManagementURL = "/tier_management"
	WebAppRoot        = "../../webapp"
	ClientAppJSBundle = "clientapp.js"
	SignOnJSBundle    = "signon.js"
	StaticJSMount     = "/assets"
	StaticImagesMount = "/images"
)

type server struct {
	*gin.Engine
	mothership mothership.Mothership
	args       serverArgs
	db         *gorm.DB
	wpManifest map[string]string // output of webpack manifest
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

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	if err != nil {
		return server{}, err
	}

	wpManifest, err := readWebpackManifest()
	if err != nil {
		return server{}, err
	}

	s := server{
		Engine:     gin.Default(),
		mothership: m,
		args:       args,
		db:         db,
		wpManifest: wpManifest,
	}
	if err := s.SetTrustedProxies(nil); err != nil {
		return server{}, err
	}
	s.setupMiddlewares()
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

func readWebpackManifest() (manifest map[string]string, err error) {
	bytes, err := os.ReadFile(WebAppRoot + "/dist/manifest.json")
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &manifest)
	if err != nil {
		return nil, err
	}
	if _, ok := manifest[ClientAppJSBundle]; !ok {
		return nil, errors.New("no clientapp js bundle in manifest")
	}
	if _, ok := manifest[SignOnJSBundle]; !ok {
		return nil, errors.New("no signon js bundle in manifest")
	}

	return manifest, nil
}

func (s *server) setupMiddlewares() {
	s.Use(gzip.Gzip(gzip.DefaultCompression))

	store := cookie.NewStore([]byte(s.args.SessionKey))
	s.Use(sessions.Sessions("mysession", store))

	s.Use(ginL.WithFlashMessage)
}

func (s *server) setupRouter() {
	s.LoadHTMLGlob("mothership/templates/**/*.tmpl")
	s.Static(StaticImagesMount, WebAppRoot+"/images")
	s.Static(StaticJSMount, WebAppRoot+"/dist")

	s.GET("/signup", s.SignOnGet)
	s.GET(SignInURL, s.SignOnGet)
	s.GET("/forgot_password", s.SignOnGet)
	s.GET("/reset_password", s.SignOnGet)

	s.POST("/signup", s.SignUp)
	s.POST(SignInURL, s.SignIn)
	s.POST("/forgot_password", s.ForgotPassword)
	s.POST("/reset_password", s.ResetPassword)
	s.GET("/confirm_user", s.ConfirmUser)
	s.POST("/resend_confirmation_email", s.ResendConfirmationEmail)

	auth := s.Group("/", ginL.AuthenticationRequired(s.db, SignInURL))
	auth.GET("/onboard", s.Onboard)

	onboarded := auth.Group("/", ginL.Onboarded(s.db))
	onboarded.GET("/", s.Home)
	onboarded.GET(TierManagementURL, s.TierManagement)
	onboarded.GET("/settings", s.Settings)

	tier := onboarded.Group("/tier/:id", ginL.TierPermission(s.db, SignInURL))
	tier.GET("/", s.Dashboard)
	tier.GET("/dashboard", s.Dashboard)
	tier.GET("/data", s.Data)
	tier.GET("/endpoints", s.Endpoints)
	tier.GET("/profiles", s.Profiles)
	tier.GET("/actions", s.Actions)
	tier.GET("/features", s.Features)
	tier.GET("/stored_queries", s.StoredQueries)
	metrics := tier.Group("/metrics")
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

func (s *server) signOnBundlePath() string {
	wpManifest := s.wpManifest
	if s.isDev() {
		wpManifest, _ = readWebpackManifest()
	}

	return StaticJSMount + "/" + wpManifest[SignOnJSBundle]
}

func (s *server) clientAppBundlePath() string {
	wpManifest := s.wpManifest
	if s.isDev() {
		wpManifest, _ = readWebpackManifest()
	}

	return StaticJSMount + "/" + wpManifest[ClientAppJSBundle]
}

func (s *server) SignOnGet(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	c.HTML(http.StatusOK, "bridge/sign_on.tmpl", gin.H{
		"flashMsg":         c.GetStringMapString(ginL.FlashMessageKey),
		"signOnBundlePath": s.signOnBundlePath(),
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
		ginL.RespondError(c, err, "send the reset password email")
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
		ginL.RespondError(c, err, "reset password")
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
		ginL.AddFlashMessage(session, ginL.FlashTypeError, msgDesc)
		c.Redirect(http.StatusFound, SignInURL)
		return
	}
	ginL.AddFlashMessage(session, ginL.FlashTypeSuccess, "Your email address has been confirmed! You can now sign in.")
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
		ginL.RespondError(c, err, "sign up")
		return
	}
	if _, err = userC.SendConfirmationEmail(ctx, s.db, s.sendgridClient(), user, &s.mothership); err != nil {
		ginL.RespondError(c, err, "send confirmation email")
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

	ginL.SaveUserIntoCookie(sessions.Default(c), user)
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

func (s *server) Home(c *gin.Context) {
	user, _ := ginL.CurrentUser(c)

	tiers, err := tierC.FetchTiers(c.Request.Context(), s.db, user.CustomerID)
	if err != nil {
		ginL.RespondError(c, err, "fetch tiers")
		return
	}
	if len(tiers) == 0 {
		c.Redirect(http.StatusFound, TierManagementURL)
		return
	}
	tier := tiers[0]
	c.Redirect(http.StatusFound, tierDashboardURL(tier))
}

func tierDashboardURL(tier tierL.Tier) string {
	return fmt.Sprintf("/tier/%v/dashboard", tier.ID)
}

func (s *server) Onboard(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	c.HTML(http.StatusOK, "bridge/index.tmpl", s.bootstrapData(c, "Onboard"))
}

func (s *server) Dashboard(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	c.HTML(http.StatusOK, "bridge/index.tmpl", s.bootstrapData(c, "Dashboard"))
}

func (s *server) bootstrapData(c *gin.Context, page string) gin.H {
	user, _ := ginL.CurrentUser(c)

	return gin.H{
		"title":               title(page),
		"user":                serializerL.User2J(user),
		"tiers":               serializerL.CustomerTiers2J(s.db, user.CustomerID),
		"clientAppBundlePath": s.clientAppBundlePath(),
	}
}

func (s *server) Data(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	c.HTML(http.StatusOK, "bridge/index.tmpl", s.bootstrapData(c, "Data"))
}

func (s *server) Endpoints(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	c.HTML(http.StatusOK, "bridge/index.tmpl", s.bootstrapData(c, "Endpoints"))
}

func (s *server) Settings(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	c.HTML(http.StatusOK, "bridge/index.tmpl", s.bootstrapData(c, "Settings"))
}

func (s *server) TierManagement(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	c.HTML(http.StatusOK, "bridge/index.tmpl", s.bootstrapData(c, "Tier Management"))
}

func (s *server) Tiers(c *gin.Context) {
	user, _ := ginL.CurrentUser(c)

	tiers, err := tierC.FetchTiers(c.Request.Context(), s.db, user.CustomerID)
	if err != nil {
		ginL.RespondError(c, err, "fetch tiers")
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"tiers": lo.Map(tiers, func(tier tierL.Tier, _ int) gin.H {
			return serializerL.Tier2M(tier, tier.DataPlane)
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

	tier, _ := ginL.CurrentTier(c)
	profiles, err := profileC.Profiles(c.Request.Context(), tier, form.Otype, form.Oid, form.Pagination)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Fail to read profiles, please try again later.",
		})
		log.Printf("Failed to read profiles: %v\n", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"profiles": lo.Map(profiles, func(profile profileL.ProfileItem, _ int) gin.H {
			return gin.H{
				"OType":      profile.OType,
				"Oid":        profile.Oid,
				"Key":        profile.Key,
				"Value":      profile.Value.String(),
				"UpdateTime": profile.UpdateTime,
			}
		}),
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

	tier, _ := ginL.CurrentTier(c)
	actions, err := actionC.Actions(c.Request.Context(), tier, form.ActionType, form.ActorType, form.ActorID, form.TargetType, form.TargetID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Fail to read actions, please try again later.",
		})
		log.Printf("Failed to read actions: %v\n", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"actions": lo.Map(actions, func(action actionL.Action, _ int) gin.H {
			return gin.H{
				"ActionID":   strconv.FormatUint(uint64(action.ActionID), 10),
				"ActorID":    action.ActorID,
				"ActorType":  action.ActorType,
				"TargetID":   action.TargetID,
				"TargetType": action.TargetType,
				"ActionType": action.ActionType,
				"Timestamp":  action.Timestamp,
				"RequestID":  action.RequestID,
				"Metadata":   action.Metadata.String(),
			}
		}),
	})
}

func (s *server) StoredQueries(c *gin.Context) {
	tier, _ := ginL.CurrentTier(c)
	queries, err := queryC.ListQueries(c.Request.Context(), tier)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Fail to read stored queries, please try again later.",
		})
		log.Printf("Failed to read stored queries: %v\n", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"queries": queries,
	})
}

func (s *server) Features(c *gin.Context) {
	tier, _ := ginL.CurrentTier(c)
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
	if user, ok := ginL.CurrentUser(c); ok {
		if _, err := userC.Logout(c.Request.Context(), s.db, user); err != nil {
			ginL.RespondError(c, err, "log out the user")
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{})
}

func (s *server) User(c *gin.Context) {
	user, ok := ginL.CurrentUser(c)
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
	user, _ := ginL.CurrentUser(c)
	result := s.db.Take(&customer, user.CustomerID)

	if result.RowsAffected == 0 {
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"error": "No team found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"team": serializerL.TeamMembers2M(s.db, customer),
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

	if user, ok := ginL.CurrentUser(c); ok {
		if err := userC.UpdateUserNames(c.Request.Context(), s.db, user, form.FirstName, form.LastName); err != nil {
			ginL.RespondError(c, err, "update user names")
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

	if user, ok := ginL.CurrentUser(c); ok {
		if _, err := userC.UpdatePassword(c.Request.Context(), s.db, user, form.CurrentPassword, form.NewPassword); err != nil {
			ginL.RespondError(c, err, "update user password")
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{})
}

func (s *server) OnboardTeamMatch(c *gin.Context) {
	user, _ := ginL.CurrentUser(c)
	matched, team, isPersonalDomain := onboardC.TeamMatch(c.Request.Context(), s.db, user)
	if matched {
		c.JSON(http.StatusOK, gin.H{
			"matched":          matched,
			"team":             serializerL.TeamMembers2M(s.db, team),
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
	user, _ := ginL.CurrentUser(c)
	_, err := onboardC.CreateTeam(c.Request.Context(), s.db, form.Name, form.AllowAutoJoin, &user)
	if err != nil {
		ginL.RespondError(c, err, "create team (onboard)")
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
	user, _ := ginL.CurrentUser(c)
	err := onboardC.JoinTeam(c.Request.Context(), s.db, form.TeamID, &user)
	if err != nil {
		ginL.RespondError(c, err, "join team (onboard)")
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"onboardStatus": user.OnboardStatus,
	})
}

func (s *server) OnboardAssignTier(c *gin.Context) {
	user, _ := ginL.CurrentUser(c)
	tier, available, err := onboardC.AssignTier(c.Request.Context(), s.db, &user)
	if err == nil && !available {
		err = errors.New("no available pre-provisioned tier")
	}
	if err != nil {
		ginL.RespondError(c, err, "assign a tier (onboard)")
		return
	}
	if available {
		var dp dataplaneL.DataPlane
		_ = s.db.Take(&dp, tier.DataPlaneID)
		c.JSON(http.StatusOK, gin.H{
			"onboardStatus": user.OnboardStatus,
			"tier":          serializerL.Tier2M(tier, dp),
		})
	}
}

func (s *server) OnboardTier(c *gin.Context) {
	user, _ := ginL.CurrentUser(c)
	tier, err := onboardC.FetchTier(c.Request.Context(), s.db, user.CustomerID)
	if err != nil {
		ginL.RespondError(c, err, "assign a tier (onboard)")
		return
	}
	var dp dataplaneL.DataPlane
	_ = s.db.Take(&dp, tier.DataPlaneID)
	c.JSON(http.StatusOK, gin.H{
		"tier": serializerL.Tier2M(tier, dp),
	})
}

func (s *server) OnboardTierProvisioned(c *gin.Context) {
	user, _ := ginL.CurrentUser(c)
	if err := onboardC.TierProvisioned(c.Request.Context(), s.db, &user); err != nil {
		ginL.RespondError(c, err, "assign a tier (onboard)")
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
	tier, _ := ginL.CurrentTier(c)

	var req queryRangeRequest
	if err := c.ShouldBind(&req); err != nil {
		ginL.RespondError(c, err, "parse query range metrics params")
		return
	}
	start, end, step, err := req.parseParams()
	if err != nil {
		ginL.RespondError(c, err, "parse query range metrics params")
		return
	}

	result, err := metricC.QueryRange(c.Request.Context(), s.db, tier, req.Query, start, end, step)
	if err != nil {
		ginL.RespondError(c, err, "query range metrics")
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
