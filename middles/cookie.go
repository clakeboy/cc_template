package middles

import (
	"github.com/clakeboy/golib/httputils"
	"github.com/gin-gonic/gin"
)

func Cookie() gin.HandlerFunc {
	return func(c *gin.Context) {
		cookie := httputils.NewHttpCookie(c.Request, c.Writer, nil)
		c.Set("cookie", cookie)
		c.Next()
	}
}
