package controllers

import (
	"cc_template/models"
	"encoding/json"
	"github.com/clakeboy/golib/utils"
	"github.com/gin-gonic/gin"
	"net/http"
	"time"
)

// UserController 管理人员控制器
type UserController struct {
	c *gin.Context
}

func NewUserController(c *gin.Context) *UserController {
	user := AuthUserToLogin(c)
	if user == nil || user.Manage != 1 {
		c.Redirect(http.StatusFound, "/404")
		return nil
	}
	return &UserController{c: c}
}

// ActionQuery 查询
func (m *UserController) ActionQuery(args []byte) (*models.QueryResult[models.UserData], error) {
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
	model := models.NewUserModel(nil)
	res, err := model.Query(params.Page, params.Number, where...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// ActionSave 保存
func (m *UserController) ActionSave(args []byte) error {
	var params struct {
		Data *models.UserData `json:"data"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}

	saveData := params.Data

	model := models.NewUserModel(nil)

	if saveData.Id == 0 {
		saveData.CreatedDate = time.Now().Unix()
		if saveData.Init == 0 {
			saveData.Passwd = "1230123"
		}
		saveData.Passwd = utils.EncodeMD5(saveData.Passwd)
		return model.Save(saveData)
	}

	orgData, err := model.GetById(saveData.Id)
	if err != nil {
		return err
	}

	orgData.Name = saveData.Name
	orgData.Manage = saveData.Manage
	orgData.Init = saveData.Init
	if saveData.Init == 0 {
		err := model.UpdateField(&models.UserData{Id: saveData.Id}, "Init", 0)
		if err != nil {
			return err
		}
		saveData.Passwd = "1230123"
	}
	if saveData.Passwd != "" {
		orgData.Passwd = utils.EncodeMD5(saveData.Passwd)
	}
	orgData.ModifiedDate = time.Now().Unix()
	return model.Update(orgData)
}

// ActionFind 查找用户
func (m *UserController) ActionFind(args []byte) (*models.UserData, error) {
	var params struct {
		Id int `json:"id"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}

	model := models.NewUserModel(nil)
	data, err := model.GetById(params.Id)
	if err != nil {
		return nil, err
	}
	data.Passwd = ""
	return data, err
}

// ActionDelete 删除
func (m *UserController) ActionDelete(args []byte) error {
	var params struct {
		Id int `json:"id"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}

	model := models.NewUserModel(nil)
	err = model.DeleteStruct(&models.UserData{
		Id: params.Id,
	})

	return err
}
