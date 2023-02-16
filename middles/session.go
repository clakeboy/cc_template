package middles

import (
	"github.com/clakeboy/golib/httputils"
	"github.com/gin-gonic/gin"
)

func Session() gin.HandlerFunc {

	return func(c *gin.Context) {
		cookie, ok := c.Get("cookie")
		if !ok {
			cookie = httputils.NewHttpCookie(c.Request, c.Writer, nil)
		}
		session := httputils.NewHttpSession(cookie.(*httputils.HttpCookie))
		session.Start()
		c.Set("session", session)
		c.Next()
		session.Flush()
	}
}
