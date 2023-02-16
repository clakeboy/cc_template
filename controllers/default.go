package controllers

import (
	"cc_template/components"
	"cc_template/models"
	"github.com/asdine/storm/q"
	"github.com/clakeboy/golib/httputils"
	"github.com/clakeboy/golib/utils"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
	"time"
)

// DefaultController 控制器
type DefaultController struct {
	c *gin.Context
}

func NewDefaultController(c *gin.Context) *DefaultController {
	return &DefaultController{c: c}
}

func (d *DefaultController) ActionIndex() {
	user := AuthUserToLogin(d.c)
	if user == nil {
		return
	}
	if user.Init == 0 {
		d.c.Redirect(http.StatusFound, "/front/def/change_passwd")
		return
	}
	model := models.NewItemModel(nil)
	openList, _ := model.List(1, 10, q.Eq("IsOver", 0))
	overList, _ := model.List(1, 10, q.Eq("IsOver", 1))
	d.c.HTML(200, "front/index.tpl", utils.M{
		"title":     "竞猜游戏",
		"user":      user,
		"open_list": openList,
		"over_list": overList,
	})
}

// ActionUser 用户首页
func (d *DefaultController) ActionUser() {
	user := AuthUserToLogin(d.c)
	if user == nil {
		return
	}
	var count float64
	model := models.NewBetModel(nil)
	betList, _ := model.GetUserBetList(user.Id, 1, 100)
	if betList != nil {
		for _, v := range betList {
			count += v.VictoryPlace
		}
	}
	userModel := models.NewUserModel(nil)
	userModel.UpdateField(user, "VictoryPlace", count)
	d.c.HTML(200, "front/user.tpl", utils.M{
		"title":    "用户中心",
		"user":     user,
		"bet_list": betList,
		"count":    count,
	})
}

// ActionLogin 登录
func (d *DefaultController) ActionLogin() {
	//user := AuthUserToLogin(d.c)
	//if user != nil {
	//	d.c.Redirect(http.StatusFound, "/front/def/index")
	//	return
	//}
	user, _ := components.AuthUser(d.c)
	if user != nil {
		d.c.Redirect(http.StatusFound, "/front/def/index")
		return
	}
	d.c.HTML(200, "front/login.tpl", utils.M{
		"title": "登录",
	})
}

// ActionLoginOut 退出登录
func (d *DefaultController) ActionLoginOut() {
	cookie := d.c.MustGet("cookie").(*httputils.HttpCookie)
	cookie.Delete(components.CookieName)
	d.c.Redirect(http.StatusFound, "/front/def/login")
}

// ActionChangePasswd 修改密码
func (d *DefaultController) ActionChangePasswd() {
	user := AuthUserToLogin(d.c)
	if user == nil {
		return
	}
	d.c.HTML(200, "front/up_passwd.tpl", utils.M{
		"title": "修改密码",
		"user":  user,
	})
}

// ActionItems 竞猜详情
func (d *DefaultController) ActionItems() {
	user := AuthUserToLogin(d.c)
	if user == nil {
		return
	}
	if user.Init == 0 {
		d.c.Redirect(http.StatusFound, "/front/def/change_passwd")
		return
	}
	getId := d.c.Query("i")
	id, err := strconv.Atoi(getId)
	var item *models.ItemData
	var bet *models.BetData
	if err != nil {
		d.c.Redirect(http.StatusFound, "/front/def/index")
		return
	}
	model := models.NewItemModel(nil)
	item, err = model.GetById(id)
	if err != nil {
		d.c.Redirect(http.StatusFound, "/front/def/index")
		return
	}
	betModel := models.NewBetModel(nil)
	bet, err = betModel.GetByItemUserId(user.Id, item.Id)
	d.c.HTML(200, "front/items.tpl", utils.M{
		"title": "竞猜详情",
		"user":  user,
		"item":  item,
		"bet":   bet,
		//"game_start": item.GameTime < time.Now().Unix(),//是否已开赛
		"game_start": false,
	})
}

func (d *DefaultController) ActionInit() {
	model := models.NewUserModel(nil)
	user, err := model.GetByName("clake")
	if err != nil {
		user = &models.UserData{
			Id:           0,
			Name:         "clake",
			Passwd:       utils.EncodeMD5("1230123"),
			Manage:       1,
			Init:         0,
			CreatedDate:  time.Now().Unix(),
			ModifiedDate: 0,
		}
		err = model.Save(user)
		if err != nil {
			d.c.String(200, "%v", err)
			return
		}
		d.c.String(200, "init user success")
		return
	}
	d.c.Redirect(302, "/front/def/index")
}

func (d *DefaultController) ActionItemStat() {
	itemModel := models.NewItemModel(nil)
	list, err := itemModel.List(1, 1000)
	if err != nil {
		d.c.String(200, "%v", err)
		return
	}
	betModel := models.NewBetModel(nil)
	for _, item := range list {
		betList, err := betModel.GetItemBetList(item.Id, 1, 1000)
		if err != nil {
			d.c.String(200, "%v", err)
			return
		}
		place := 0.0
		for _, bet := range betList {
			place += bet.VictoryPlace
		}
		err = itemModel.UpdateField(item, "VictoryPlace", place)
		if err != nil {
			d.c.String(200, "%v", err)
			return
		}
	}
	d.c.String(200, "item calc done")
}

func (d *DefaultController) ActionUserStat() {
	userModel := models.NewUserModel(nil)
	list, err := userModel.List(1, 1000)
	if err != nil {
		d.c.String(200, "%v", err)
		return
	}
	betModel := models.NewBetModel(nil)
	for _, user := range list {
		betList, err := betModel.GetUserBetList(user.Id, 1, 1000)
		if err != nil {
			continue
		}
		place := 0.0
		for _, bet := range betList {
			place += bet.VictoryPlace
		}
		err = userModel.UpdateField(user, "VictoryPlace", place)
		if err != nil {
			d.c.String(200, "update user VictoryPlace error: %v", err)
			return
		}
	}
	d.c.String(200, "user calc done")
}
