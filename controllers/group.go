package controllers

import (
	"cc_template/common"
	"cc_template/models"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// 控制器
type GroupController struct {
	c   *gin.Context
	acc *models.AccountData
}

func NewGroupController(c *gin.Context) *GroupController {
	Account := AuthAccountLogin(c)
	if Account == nil || Account.Manage != 1 {
		c.Redirect(http.StatusFound, "/404")
		return nil
	}
	return &GroupController{c: c, acc: Account}
}

// 查询
func (g *GroupController) ActionQuery(args []byte) (*models.QueryResult[models.GroupData], error) {
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
	model := models.NewGroupModel(nil)
	res, err := model.Query(params.Page, params.Number, where...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// 添加
func (g *GroupController) ActionSave(args []byte) error {
	var params struct {
		Data *models.GroupData `json:"data"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}
	model := models.NewGroupModel(nil)

	if params.Data.Id == 0 {
		params.Data.CreatedDate = time.Now().Unix()
		params.Data.CreatedBy = g.acc.Name
		return model.Save(params.Data)
	}

	orgData, err := model.GetById(params.Data.Id)
	if err != nil {
		return err
	}
	orgData.Name = params.Data.Name
	orgData.MenuList = params.Data.MenuList
	orgData.ModifiedBy = g.acc.Name
	orgData.ModifiedDate = time.Now().Unix()
	err = model.Update(orgData)
	if err != nil {
		common.MemCache.Delete("grp_" + orgData.Name)
	}
	return err
}

// 查找一条记录
func (g *GroupController) ActionFind(args []byte) (*models.GroupData, error) {
	var params struct {
		GroupId int `json:"id"`
	}
	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}
	model := models.NewGroupModel(nil)
	data, err := model.GetById(params.GroupId)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// 删除
func (g *GroupController) ActionDelete(args []byte) error {
	return nil
}
