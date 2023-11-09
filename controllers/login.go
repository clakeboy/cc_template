package controllers

import (
	"encoding/json"
	"fmt"
	"pcbx-btm/common"
	"pcbx-btm/components"
	"pcbx-btm/models"
	"strconv"
	"strings"

	"github.com/clakeboy/golib/httputils"
	"github.com/clakeboy/golib/utils"
	"github.com/gin-gonic/gin"
	"github.com/wenlng/go-captcha/captcha"
)

// LoginController 登录控制器
type LoginController struct {
	c *gin.Context
}

func NewLoginController(c *gin.Context) *LoginController {
	return &LoginController{c: c}
}

// ActionAuth 验证是否已登录
func (l *LoginController) ActionAuth(args []byte) (*models.AccountData, error) {
	Account, err := components.AuthAccount(l.c)
	if err != nil {
		return nil, err
	}
	Account.Passwd = ""
	return Account, nil
}

// ActionLogin 登录
func (l *LoginController) ActionLogin(args []byte) (*models.AccountData, error) {
	var params struct {
		Username string `json:"Username"`
		Password string `json:"password"`
		CaptKey  string `json:"capt_key"`
	}
	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}

	if params.Username == "" {
		return nil, fmt.Errorf("用户名或密码错误")
	}

	chkCapt, err := common.MemCache.Get(params.CaptKey + "_success")
	if err != nil || !chkCapt.(bool) {
		return nil, fmt.Errorf("请先通过人机验证")
	}
	manager := new(models.AccountData)
	model := models.NewAccountModel(nil)
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
	model := models.NewAccountModel(nil)
	orgData, err := model.GetById(params.Id)
	if err != nil {
		return err
	}
	orgData.Passwd = utils.EncodeMD5(params.Password)
	orgData.Init = 1
	return model.Update(orgData)
}

var captchaChars = "123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

func (l *LoginController) ActionGetCaptchaData(args []byte) (utils.M, error) {
	var params struct {
		Language string `json:"lng"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}
	capt := captcha.GetCaptcha()
	if params.Language == "en" {
		capt.SetRangChars(strings.Split(captchaChars, ""))
		capt.SetRangFontSize(captcha.RangeVal{Min: 32, Max: 38})
	} else {
		capt.SetRangChars(*captcha.GetCaptchaDefaultChars())
	}
	dots, b64, tb64, key, err := capt.Generate()
	if err != nil {
		return utils.M{
			"code":    1,
			"message": "GenCaptcha err",
		}, err
	}
	common.MemCache.Set(key, dots, 300)
	//writeCache(dots, key)
	return utils.M{
		"code":         0,
		"image_base64": b64,
		"thumb_base64": tb64,
		"captcha_key":  key,
	}, nil
}

func (l *LoginController) ActionCheckCaptcha(args []byte) (utils.M, error) {
	code := 1
	var params struct {
		Dots string `json:"dots"`
		Key  string `json:"key"`
	}
	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}
	dots := params.Dots
	key := params.Key
	if dots == "" || key == "" {
		return utils.M{
			"code":    code,
			"message": "dots or key param is empty",
		}, nil
	}

	cacheData, err := common.MemCache.Get(key)
	if err != nil {
		return utils.M{
			"code":    code,
			"message": "illegal key",
		}, nil
	}
	src := strings.Split(dots, ",")

	dct := cacheData.(map[int]captcha.CharDot)

	chkRet := false
	if (len(dct) * 2) == len(src) {
		for i, dot := range dct {
			j := i * 2
			k := i*2 + 1
			sx, _ := strconv.ParseFloat(fmt.Sprintf("%v", src[j]), 64)
			sy, _ := strconv.ParseFloat(fmt.Sprintf("%v", src[k]), 64)

			// 检测点位置
			// chkRet = captcha.CheckPointDist(int64(sx), int64(sy), int64(dot.Dx), int64(dot.Dy), int64(dot.Width), int64(dot.Height))

			// 校验点的位置,在原有的区域上添加额外边距进行扩张计算区域,不推荐设置过大的padding
			// 例如：文本的宽和高为30，校验范围x为10-40，y为15-45，此时扩充5像素后校验范围宽和高为40，则校验范围x为5-45，位置y为10-50
			chkRet = captcha.CheckPointDistWithPadding(int64(sx), int64(sy), int64(dot.Dx), int64(dot.Dy), int64(dot.Width), int64(dot.Height), 5)
			if !chkRet {
				break
			}
		}
	}

	if chkRet {
		// 通过校验
		common.MemCache.Delete(key)
		common.MemCache.Set(key+"_success", true, 60)
		code = 0
	}

	return utils.M{
		"code": code,
	}, nil
}
