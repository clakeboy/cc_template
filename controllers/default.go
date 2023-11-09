package controllers

import (
	"github.com/gin-gonic/gin"
)

// DefaultController 控制器
type DefaultController struct {
	c *gin.Context
}

func NewDefaultController(c *gin.Context) *DefaultController {
	return &DefaultController{c: c}
}

func (d *DefaultController) ActionIndex() {

}
