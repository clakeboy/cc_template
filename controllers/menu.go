package controllers

import (
	"cc_template/models"
	"encoding/json"
	"fmt"
	"time"

	"github.com/clakeboy/storm-rev"
	"github.com/clakeboy/storm-rev/q"
	"github.com/gin-gonic/gin"
)

// 控制器
type MenuController struct {
	c   *gin.Context
	acc *models.AccountData
}

func NewMenuController(c *gin.Context) *MenuController {
	user := AuthAccountLogin(c)
	return &MenuController{c: c, acc: user}
}

// 查询
func (m *MenuController) ActionQuery(args []byte) ([]*models.MenuData, error) {
	var params struct {
		Query  []*Condition `json:"query"`
		Page   int          `json:"page"`
		Number int          `json:"number"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}

	where := explainQueryCondition(params.Query)
	where = append(where, q.Eq("ParentId", 0))
	model := models.NewMenuModel(nil)
	model.SetOrder("Sort", "ASC")
	list, err := model.List(params.Page, params.Number, where...)
	if err != nil && err != storm.ErrNotFound {
		return nil, err
	}
	for _, v := range list {
		m.getChildMenu(v, model, nil)
	}
	return list, nil
}

// 重建索引
func (m *MenuController) ActionReindex() error {
	return models.NewMenuModel(nil).ReIndex(new(models.MenuData))
}

func (m *MenuController) getChildMenu(menu *models.MenuData, model *models.MenuModel, filter []int) {
	child, err := model.GetByParentId(menu.Id, filter)
	if err != nil {
		return
	}
	menu.Children = child
	for _, v := range child {
		m.getChildMenu(v, model, filter)
	}
}

// 查询
func (m *MenuController) ActionSearch(args []byte) ([]*models.MenuData, error) {
	var params struct {
		Text string `json:"text"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}
	where := q.Re("Text", fmt.Sprintf("^%s", params.Text))
	model := models.NewMenuModel(nil)
	model.SetOrder("Sort", "ASC")
	list, err := model.List(1, 100, where)
	if err != nil {
		return nil, err
	}

	return list, nil
}

// 添加
func (m *MenuController) ActionSave(args []byte) error {
	var params struct {
		Data *models.MenuData `json:"data"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}
	model := models.NewMenuModel(nil)

	if params.Data.Id == 0 {
		params.Data.CreatedDate = time.Now().Unix()
		params.Data.CreatedBy = m.acc.Name
		return model.Save(params.Data)
	}

	orgData, err := model.GetById(params.Data.Id)
	if err != nil {
		return err
	}
	orgData.Name = params.Data.Name
	orgData.Link = params.Data.Link
	orgData.Icon = params.Data.Icon
	orgData.ParentId = params.Data.ParentId
	orgData.ParentName = params.Data.ParentName
	orgData.Text = params.Data.Text
	orgData.Sort = params.Data.Sort
	orgData.ModifiedBy = m.acc.Name
	orgData.ModifiedDate = time.Now().Unix()
	err = model.Update(orgData)
	if params.Data.ParentId == 0 {
		model.UpdateField(orgData, "ParentId", 0)
		model.UpdateField(orgData, "ParentName", "")
	}
	return err
}

// 查询
func (m *MenuController) ActionFind(args []byte) (*models.MenuData, error) {
	var params struct {
		Id int `json:"id"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}

	model := models.NewMenuModel(nil)
	data, err := model.GetById(params.Id)
	if err != nil {
		return nil, err
	}
	return data, err
}

// 删除
func (m *MenuController) ActionDelete(args []byte) error {
	return nil
}
