package gin

import (
	"fennel/mothership/lib"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

func RespondError(c *gin.Context, err error, action string) {
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
