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
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	ginL "fennel/mothership/lib/gin"
	jsonL "fennel/mothership/lib/json"
	userL "fennel/mothership/lib/user"
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
}

func (s *server) setupRouter() {
	s.LoadHTMLGlob("mothership/templates/**/*.tmpl")
	s.Static(StaticImagesMount, WebAppRoot+"/images")
	s.Static(StaticJSMount, WebAppRoot+"/dist")

	s.GET("/signup", s.SignOnGet)
	s.GET(SignInURL, s.SignOnGet)
	s.GET("/forgot_password", s.SignOnGet)
	s.GET("/reset_password", s.SignOnGet)

	auth := s.Group("/", ginL.AuthenticationRequired(s.db, SignInURL))
	auth.GET("/", s.Dashboard)
	auth.GET("/feature/:id", s.Feature)
	auth.POST("/features", s.Features)
}

func (s *server) SignOnGet(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	c.HTML(http.StatusOK, "bridge/sign_on.tmpl", gin.H{
		"flashMsg":         c.GetStringMapString(ginL.FlashMessageKey),
		"signOnBundlePath": s.signOnBundlePath(),
	})
}

func (s *server) signOnBundlePath() string {
	wpManifest := s.wpManifest
	if s.isDev() {
		wpManifest, _ = readWebpackManifest()
	}

	return StaticJSMount + "/" + wpManifest[SignOnJSBundle]
}

func fakeUser() userL.User {
	// TODO(xiao) use real user from auth
	return userL.User{
		Email:         "xiao@fennel.ai",
		FirstName:     "Xiao",
		LastName:      "Jiang",
		OnboardStatus: userL.OnboardStatusDone,
	}
}

func (s *server) Feature(c *gin.Context) {
	c.HTML(http.StatusOK, "console/app.html.tmpl", gin.H{
		"title":                title("Feature"),
		"featureAppBundlePath": s.featureAppBundlePath(),
		"user":                 jsonL.User2J(fakeUser()),
	})
}

func (s *server) Dashboard(c *gin.Context) {
	c.HTML(http.StatusOK, "console/app.html.tmpl", gin.H{
		"title":                title("home"),
		"featureAppBundlePath": s.featureAppBundlePath(),
		"user":                 jsonL.User2J(fakeUser()),
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
