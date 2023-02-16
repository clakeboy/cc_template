package middles

import (
	"cc_template/common"
	"github.com/gin-gonic/gin"
)

func Cache() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("cache", common.MemCache)
		c.Next()
	}
}
