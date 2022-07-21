package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

var db = make(map[string]string)

const DashboardPage = "dashboard"
const DataPage = "data"

func setupRouter() *gin.Engine {
	// Disable Console Color
	// gin.DisableConsoleColor()
	r := gin.Default()

	// Ping test
	r.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	r.LoadHTMLGlob("templates/*.tmpl")
	r.Static("/images", "../../webapp/images")
	r.Static("/assets", "../../webapp/dist")
	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.tmpl", gin.H{"title": "Fennel | Dashboard", "page": DashboardPage})
	})
	r.GET("/dashboard", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.tmpl", gin.H{"title": "Fennel | Dashboard", "page": DashboardPage})
	})
	r.GET("/data", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.tmpl", gin.H{"title": "Fennel | Data", "page": DataPage})
	})

	r.GET("/profiles", func(c *gin.Context) {
		time.Sleep(3 * time.Second)
		c.JSON(http.StatusOK, gin.H{
			"profiles": []gin.H{
				{
					"otype":        "movie",
					"oid":          1,
					"key_col":      "genre",
					"last_updated": 1652296764,
					"value":        "Adventure|Animation|Children",
				},
				{
					"otype":        "movie",
					"oid":          1,
					"key_col":      "movie_title",
					"last_updated": 1652296764,
					"value":        "Toy Story",
				},
				{
					"otype":        "movie",
					"oid":          1,
					"key_col":      "release_year",
					"last_updated": 1652296764,
					"value":        "1995",
				},
			},
		})
	})

	// Get user value
	r.GET("/user/:name", func(c *gin.Context) {
		user := c.Params.ByName("name")
		value, ok := db[user]
		if ok {
			c.JSON(http.StatusOK, gin.H{"user": user, "value": value})
		} else {
			c.JSON(http.StatusOK, gin.H{"user": user, "status": "no value"})
		}
	})

	// Authorized group (uses gin.BasicAuth() middleware)
	// Same than:
	// authorized := r.Group("/")
	// authorized.Use(gin.BasicAuth(gin.Credentials{
	//	  "foo":  "bar",
	//	  "manu": "123",
	//}))
	authorized := r.Group("/", gin.BasicAuth(gin.Accounts{
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

	return r
}

func main() {
	r := setupRouter()
	// Listen and Server in 0.0.0.0:8080
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Error running the server: %s", err)
	}
}
