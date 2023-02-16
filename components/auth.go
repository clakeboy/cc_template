package components

import (
	"cc_template/models"
	"github.com/clakeboy/golib/httputils"
	"github.com/gin-gonic/gin"
	"strconv"
)

var CookieName = "gb_acc"

func AuthUser(c *gin.Context) (*models.UserData, error) {
	cookie := c.MustGet("cookie").(*httputils.HttpCookie)
	acc, err := cookie.Get(CookieName)

	if err != nil {
		return nil, err
	}

	id, err := strconv.Atoi(acc)
	if err != nil {
		return nil, err
	}

	model := models.NewUserModel(nil)
	return model.GetById(id)
}
