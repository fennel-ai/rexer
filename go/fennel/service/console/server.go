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

	jsonL "fennel/mothership/lib/json"
	userL "fennel/mothership/lib/user"
)

const (
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

	s.GET("/", s.Feature)
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
		"title":                title("home"),
		"featureAppBundlePath": s.featureAppBundlePath(),
		"user":                 jsonL.User2J(fakeUser()),
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
