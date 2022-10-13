package main

import (
	"encoding/json"
	"errors"
	"fennel/mothership"
	"fennel/service/common"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	"github.com/sendgrid/sendgrid-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	onboardC "fennel/mothership/controller/onboard"
	userC "fennel/mothership/controller/user"
	"fennel/mothership/lib"
	dataplaneL "fennel/mothership/lib/dataplane"
	ginL "fennel/mothership/lib/gin"
	jsonL "fennel/mothership/lib/json"
	serializerL "fennel/mothership/lib/serializer"
)

const (
	SignInURL          = "/signin"
	WebAppRoot         = "../../webapp"
	StaticJSMount      = "/assets"
	StaticImagesMount  = "/images"
	FeatureAppJSBundle = "featureapp.js"
	SignOnJSBundle     = "signon.js"
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
	SessionKey     string `arg:"required,--console_session_key,env:BRIDGE_SESSION_KEY"`
	SendgridAPIKey string `arg:"required,--sendgrid_api_key,env:SENDGRID_API_KEY"`
	AppPort        string `arg:"--app-port,env:APP_PORT" default:"8080"`
	GINMode        string `args:"--gin-mode,env:GIN_MODE" default:"debug"`

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
	seed := args.RandSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rand.Seed(seed)
	log.Printf("Using rand seed %d\n", seed)
	common.StartHealthCheckServer(args.HealthPort)

	s.setupMiddlewares()
	s.setupRouter()

	return s, nil
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
	auth.GET("/onboard", s.Main)

	onboarded := auth.Group("/", ginL.Onboarded(s.db))
	onboarded.GET("/", s.Main)
	onboarded.GET("/feature/:id", s.Feature)

	// ajax endpoints
	auth.POST("/logout", s.Logout)
	auth.POST("/features", s.Features)

	// onboard endpoints
	onboard := auth.Group("/onboard")
	onboard.GET("/team_match", s.OnboardTeamMatch)
	onboard.POST("/create_team", s.OnboardCreateTeam)
	onboard.POST("/join_team", s.OnboardJoinTeam)

	onboard.POST("/assign_tier", s.OnboardAssignTier)
	onboard.GET("/tier", s.OnboardTier)
	onboard.POST("/tier_provisioned", s.OnboardTierProvisioned)
}

func (s *server) SignOnGet(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	c.HTML(http.StatusOK, "bridge/sign_on.tmpl", gin.H{
		"flashMsg":         c.GetStringMapString(ginL.FlashMessageKey),
		"signOnBundlePath": s.signOnBundlePath(),
	})
}

func (s *server) Main(c *gin.Context) {
	user, _ := ginL.CurrentUser(c)
	c.Header("Cache-Control", "no-cache")
	c.HTML(http.StatusOK, "console/app.html.tmpl", gin.H{
		"featureAppBundlePath": s.featureAppBundlePath(),
		"user":                 jsonL.User2J(user),
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

func (s *server) signOnBundlePath() string {
	wpManifest := s.wpManifest
	if s.isDev() {
		wpManifest, _ = readWebpackManifest()
	}

	return StaticJSMount + "/" + wpManifest[SignOnJSBundle]
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

func (s *server) Feature(c *gin.Context) {
	user, _ := ginL.CurrentUser(c)
	c.HTML(http.StatusOK, "console/app.html.tmpl", gin.H{
		"title":                title("Feature"),
		"featureAppBundlePath": s.featureAppBundlePath(),
		"user":                 jsonL.User2J(user),
	})
}

func (s *server) Features(c *gin.Context) {
	type Filter struct {
		Type  string `form:"type"`
		Value string `form:"value"`
	}
	type Feature struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Version uint   `json:"version"`
		Tags    []string
	}
	var form struct {
		Filters []Filter `form:"filters"`
	}
	if err := c.BindJSON(&form); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	allFeatures := []Feature{
		{
			ID:   "101",
			Name: "user_avg_rating",
			Tags: []string{"good"},
		},
		{
			ID:   "102",
			Name: "movie_avg_rating",
			Tags: []string{"ok"},
		},
		{
			ID:   "103",
			Name: "user_likes_last_3days",
			Tags: []string{"bad"},
		},
		{
			ID:   "104",
			Name: "movie_likes_last_3days",
			Tags: []string{"ok"},
		},
	}
	features := make([]Feature, 0, len(allFeatures))
	for _, feature := range allFeatures {
		match := true
		for _, filter := range form.Filters {
			if filter.Type == "tag" && feature.Tags[0] != filter.Value {
				match = false
			}
			if filter.Type == "name" && feature.Name != filter.Value {
				match = false
			}
		}
		if match {
			features = append(features, feature)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"features": features,
	})
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

func (s *server) featureAppBundlePath() string {
	wpManifest := s.wpManifest
	if s.isDev() {
		wpManifest, _ = readWebpackManifest()
	}

	return StaticJSMount + "/" + wpManifest[FeatureAppJSBundle]
}

func (s *server) isDev() bool {
	return s.args.GINMode == "debug"
}

func title(name string) string {
	return fmt.Sprintf("Fennel | %s", name)
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
	if _, ok := manifest[FeatureAppJSBundle]; !ok {
		return nil, errors.New("no featureapp js bundle in manifest")
	}
	if _, ok := manifest[SignOnJSBundle]; !ok {
		return nil, errors.New("no signon js bundle in manifest")
	}

	return manifest, nil
}
