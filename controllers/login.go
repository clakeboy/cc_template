package controllers

import (
	"cc_template/components"
	"cc_template/models"
	"encoding/json"
	"fmt"
	"github.com/clakeboy/golib/httputils"
	"github.com/clakeboy/golib/utils"
	"github.com/gin-gonic/gin"
	"strconv"
)

// LoginController 登录控制器
type LoginController struct {
	c *gin.Context
}

func NewLoginController(c *gin.Context) *LoginController {
	return &LoginController{c: c}
}

// ActionAuth 验证是否已登录
func (l *LoginController) ActionAuth(args []byte) (*models.UserData, error) {
	user, err := components.AuthUser(l.c)
	if err != nil {
		return nil, err
	}
	user.Passwd = ""
	return user, nil
}

// ActionLogin 登录
func (l *LoginController) ActionLogin(args []byte) (*models.UserData, error) {
	var params struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}

	if params.Username == "" {
		return nil, fmt.Errorf("用户名或密码错误")
	}
	manager := new(models.UserData)
	model := models.NewUserModel(nil)
	err = model.One("Name", params.Username, manager)
	if err != nil {
		return nil, fmt.Errorf("用户名或密码错误")
	}
	if utils.EncodeMD5(params.Password) != manager.Passwd {
		return nil, fmt.Errorf("用户或密码错误")
	}
	manager.Passwd = ""

	cookie := l.c.MustGet("cookie").(*httputils.HttpCookie)
	cookie.Set(components.CookieName, strconv.Itoa(manager.Id), 365*24*3600)

	return manager, nil
}

// ActionLogout 退出登录
func (l *LoginController) ActionLogout(args []byte) error {
	cookie := l.c.MustGet("cookie").(*httputils.HttpCookie)
	cookie.Delete(components.CookieName)
	return nil
}

func (l *LoginController) ActionChangePassword(args []byte) error {
	var params struct {
		Id       int    `json:"id"`
		Password string `json:"password"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}
	model := models.NewUserModel(nil)
	orgData, err := model.GetById(params.Id)
	if err != nil {
		return err
	}
	orgData.Passwd = utils.EncodeMD5(params.Password)
	orgData.Init = 1
	return model.Update(orgData)
}
