package middles

import (
	"cc_template/common"
	"github.com/gin-gonic/gin"
)

func BoltDatabase() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("bolt", common.BDB)
		c.Next()
	}
}
