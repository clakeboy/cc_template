package controllers

import (
	"cc_template/common"
	"cc_template/models"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/clakeboy/golib/utils"
	"github.com/clakeboy/storm-rev"
	"github.com/gin-gonic/gin"
)

//go:embed setup/menu.json
var menuData []byte

// 控制器
type SetupController struct {
	c *gin.Context
}

func NewSetupController(c *gin.Context) *SetupController {
	return &SetupController{c: c}
}

// 查询
func (s *SetupController) ActionInit(args []byte) error {
	var params struct {
		User     string `json:"user"`     //初始化用户名
		Password string `json:"password"` //初始化密码
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}

	var init bool

	err = common.BDB.Get("setup", "init", &init)
	if err != nil && err != storm.ErrNotFound {
		return fmt.Errorf("系统初始化出错 %v", err)
	}

	if init {
		return fmt.Errorf("系统已经初始化")
	}

	err = initMenu()

	if err != nil {
		return err
	}

	grpModel := models.NewGroupModel(nil)
	err = grpModel.Save(&models.GroupData{
		Id:          1,
		Name:        "admin",
		MenuList:    []int{1, 2, 3, 4, 5, 6, 7},
		CreatedDate: time.Now().Unix(),
		CreatedBy:   "setup",
	})
	if err != nil {
		return fmt.Errorf("初始化组信息出错 %v", err)
	}
	accModel := models.NewAccountModel(nil)
	err = accModel.Save(&models.AccountData{
		GroupId:     1,
		GroupName:   "admin",
		Name:        params.User,
		Passwd:      utils.EncodeMD5(params.Password),
		Manage:      1,
		CreatedDate: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("初始化管理员信息出错 %v", err)
	}
	common.BDB.Set("setup", "init", true)
	return nil
}

// 查询系统是否已初始化
func (s *SetupController) ActionIs(args []byte) (bool, error) {
	var init bool

	err := common.BDB.Get("setup", "init", &init)
	if err != nil && err != storm.ErrNotFound {
		return false, fmt.Errorf("系统初始化出错 %v", err)
	}

	return init, nil
}

func initMenu() error {
	menuModel := models.NewMenuModel(nil)
	var list []*models.MenuData
	err := json.Unmarshal(menuData, &list)
	if err != nil {
		return fmt.Errorf("初始化菜单出错 %v", err)
	}

	for _, item := range list {
		err = menuModel.Save(item)
		if err != nil {
			return fmt.Errorf("初始化菜单出错 %v", err)
		}
	}

	return nil
}
