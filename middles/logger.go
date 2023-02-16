package middles

import (
	"github.com/clakeboy/golib/components"
	"github.com/gin-gonic/gin"
)

func Logger() gin.HandlerFunc {
	out := &components.SysLog{Prefix: "access-"}
	return gin.LoggerWithWriter(out)
}
