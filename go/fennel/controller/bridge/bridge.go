package bridge

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func Ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

func Dashboard(c *gin.Context) {
	c.HTML(http.StatusOK, "index.tmpl", gin.H{"title": "Fennel | Dashboard", "page": DashboardPage})
}

func Data(c *gin.Context) {
	c.HTML(http.StatusOK, "index.tmpl", gin.H{"title": "Fennel | Data", "page": DataPage})
}

func Profiles(c *gin.Context) {
	time.Sleep(time.Second)
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
}
