package components

import (
	"cc_template/models"
	"strconv"

	"github.com/clakeboy/golib/httputils"
	"github.com/gin-gonic/gin"
)

var CookieName = "gb_acc"

func AuthAccount(c *gin.Context) (*models.AccountData, error) {
	cookie := c.MustGet("cookie").(*httputils.HttpCookie)
	acc, err := cookie.Get(CookieName)

	if err != nil {
		return nil, err
	}

	id, err := strconv.Atoi(acc)
	if err != nil {
		return nil, err
	}

	model := models.NewAccountModel(nil)
	return model.GetById(id)
}
