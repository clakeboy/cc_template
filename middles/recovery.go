package middles

import (
	"github.com/clakeboy/golib/components"
	"github.com/gin-gonic/gin"
)

func Recovery() gin.HandlerFunc {
	out := &components.SysLog{Prefix: "error-"}
	return gin.RecoveryWithWriter(out)
}
