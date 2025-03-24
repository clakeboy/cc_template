package router

import (
	"cc_template/controllers"

	"github.com/gin-gonic/gin"
)

func GetController(controllerName string, c *gin.Context) interface{} {
	switch controllerName {
	case "def":
		return controllers.NewDefaultController(c)
	case "login":
		return controllers.NewLoginController(c)
	case "acc":
		return controllers.NewAccountController(c)
	default:
		return nil
	}
}
