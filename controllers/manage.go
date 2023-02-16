package controllers

import (
	"cc_template/components"
	"cc_template/models"
	"github.com/clakeboy/golib/utils"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

// ManageController 管理控制器
type ManageController struct {
	c    *gin.Context
	user *models.UserData
}

func NewManageController(c *gin.Context) *ManageController {
	user, _ := components.AuthUser(c)
	if user == nil || user.Manage != 1 {
		c.Redirect(http.StatusFound, "/front/def/index")
		return nil
	}
	return &ManageController{c: c, user: user}
}

// ActionIndex 首页
func (m *ManageController) ActionIndex() {
	if m == nil {
		return
	}
	m.c.HTML(200, "manage/index.tpl", utils.M{
		"title": "管理首页",
	})
}

// ActionItems 竞猜游戏列表
func (m *ManageController) ActionItems() {
	model := models.NewItemModel(nil)
	list, err := model.List(1, 1000)
	count := 0.0
	for _, v := range list {
		count += v.VictoryPlace
	}
	m.c.HTML(200, "manage/items.tpl", gin.H{
		"title":  "竞猜游戏列表",
		"list":   list,
		"msg":    err,
		"count":  count,
		"format": FormatDate,
	})
}

// ActionItemDetail 竞猜游戏详情
func (m *ManageController) ActionItemDetail() {
	getId := m.c.Query("i")
	id, err := strconv.Atoi(getId)
	var item *models.ItemData
	if err == nil {
		model := models.NewItemModel(nil)
		item, err = model.GetById(id)
	}
	m.c.HTML(200, "manage/item_detail.tpl", utils.M{
		"title":   utils.YN(item != nil, "修改竞猜游戏", "添加竞猜游戏").(string),
		"vd_list": models.ItemConditionList,
		"content": item,
		"is_edit": item != nil,
		"msg":     err,
	})
}

// ActionItemOver 结束竞猜页
func (m *ManageController) ActionItemOver() {
	getId := m.c.Query("i")
	id, err := strconv.Atoi(getId)
	var item *models.ItemData
	if err != nil {
		m.c.Redirect(http.StatusFound, "/front/manage/items")
		return
	}
	model := models.NewItemModel(nil)
	item, err = model.GetById(id)
	if err != nil {
		m.c.Redirect(http.StatusFound, "/front/manage/items")
		return
	}
	m.c.HTML(200, "manage/item_over.tpl", utils.M{
		"title": "结束竞猜",
		"item":  item,
	})
}

// ActionItemBet 显示比赛竞猜列表
func (m *ManageController) ActionItemBet() {
	getId := m.c.Query("i")
	id, err := strconv.Atoi(getId)
	if err != nil {
		m.c.Redirect(http.StatusFound, "/front/manage/items")
		return
	}
	var item *models.ItemData
	model := models.NewItemModel(nil)
	item, err = model.GetById(id)
	if err != nil {
		m.c.Redirect(http.StatusFound, "/front/manage/items")
		return
	}
	betModel := models.NewBetModel(nil)
	betList, _ := betModel.GetItemBetList(item.Id, 1, 100)
	count := 0
	for _, v := range betList {
		count += int(v.VictoryPlace)
	}
	m.c.HTML(200, "manage/item_bet.tpl", utils.M{
		"title":     "竞猜用户列表",
		"item":      item,
		"bet_list":  betList,
		"bet_count": count,
		"user":      m.user,
	})
}

// ActionUsers 用户列表
func (m *ManageController) ActionUsers() {
	model := models.NewUserModel(nil)
	qres, err := model.Query(1, 100)
	m.c.HTML(200, "manage/users.tpl", gin.H{
		"title":  "用户列表",
		"qres":   qres,
		"msg":    err,
		"format": FormatDate,
	})
}

// ActionUserDetail 用户详情
func (m *ManageController) ActionUserDetail() {
	getId := m.c.Query("i")
	id, err := strconv.Atoi(getId)
	var user *models.UserData
	if err == nil {
		model := models.NewUserModel(nil)
		user, err = model.GetById(id)
	}
	m.c.HTML(200, "manage/user_detail.tpl", utils.M{
		"title":   utils.YN(user != nil, "修改用户", "添加用户").(string),
		"content": user,
		"is_edit": user != nil,
		"msg":     err,
	})
}

// ActionBetDetail 竞猜详情
func (m *ManageController) ActionBetDetail() {
	getId := m.c.Query("i")
	id, err := strconv.Atoi(getId)
	if err != nil {
		m.c.Redirect(http.StatusFound, "/front/def/index")
		return
	}
	model := models.NewBetModel(nil)
	data, err := model.GetById(id)
	if err != nil {
		m.c.Redirect(http.StatusFound, "/front/def/index")
		return
	}
	itemModel := models.NewItemModel(nil)
	itemData, err := itemModel.GetById(data.ItemId)
	if err != nil {
		m.c.Redirect(http.StatusFound, "/front/def/index")
		return
	}
	m.c.HTML(200, "manage/bet_detail.tpl", utils.M{
		"title": "竞猜详情",
		"bet":   data,
		"item":  itemData,
		"user":  m.user,
	})
}
