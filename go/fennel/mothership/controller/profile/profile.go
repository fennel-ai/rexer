package profile

import (
	"encoding/json"
	"fennel/client"
	"fennel/lib/sql"
	"net/http"

	"github.com/gin-gonic/gin"
)

func Profiles(c *gin.Context) {
	var filter sql.CompositeSqlFilter
	// We expect filter type in the body.
	// c.BindJSON(&filter)
	// For now just using a dummy filter to see if things work e2e.
	str := `{
		"Left": "OType",
		"Op": "=",
		"Right": "channel"
	}
		`
	err := json.Unmarshal([]byte(str), &filter)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	cli, err := client.NewClient("http://localhost:2425", http.DefaultClient)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	profiles, err := cli.QueryProfiles(&filter)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	result := make([]gin.H, len(profiles))
	for i, p := range profiles {
		result[i] = map[string]any{
			"otype":        p.OType,
			"oid":          p.Oid,
			"key_col":      p.Key,
			"last_updated": p.UpdateTime,
			"value":        p.Value,
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"profiles": result,
	})
}
