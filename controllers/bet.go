package controllers

import (
	"cc_template/models"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"time"
)

// 控制器
type BetController struct {
	c *gin.Context
}

func NewBetController(c *gin.Context) *BetController {
	return &BetController{c: c}
}

// ActionQuery 查询
func (b *BetController) ActionQuery(args []byte) (*models.QueryResult[models.BetData], error) {
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
	model := models.NewBetModel(nil)
	res, err := model.Query(params.Page, params.Number, where...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// ActionDo 用户竞猜
func (b *BetController) ActionDo(args []byte) error {
	user := AuthUserToLogin(b.c)
	if user == nil {
		return nil
	}
	var params struct {
		ItemId   int   `json:"item_id"`
		BetType  int   `json:"bet_type"`
		BetPlace int64 `json:"bet_place"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}
	var data *models.BetData
	itemModel := models.NewItemModel(nil)
	item, err := itemModel.GetById(params.ItemId)
	if err != nil {
		return err
	}
	//if item.GameTime < time.Now().Unix() {
	//	return fmt.Errorf("开赛时间已过，不能竞猜")
	//}
	betModel := models.NewBetModel(nil)
	data, err = betModel.GetByItemUserId(user.Id, item.Id)
	if err != nil {
		data = &models.BetData{
			Id:            0,
			ItemId:        item.Id,
			ItemRightName: item.RightName,
			ItemLeftName:  item.LeftName,
			ItemGameTime:  item.GameTime,
			UserId:        user.Id,
			UserName:      user.Name,
			BetType:       params.BetType,
			BetPlace:      params.BetPlace,
			VictoryPlace:  0,
			ModifiedDate:  0,
			CreatedDate:   time.Now().Unix(),
		}
		return betModel.Save(data)
	}
	data.ItemRightName = item.RightName
	data.ItemLeftName = item.LeftName
	data.ItemGameTime = item.GameTime
	data.BetType = params.BetType
	data.BetPlace = params.BetPlace
	data.ModifiedDate = time.Now().Unix()
	return betModel.Update(data)
}

// ActionEdit 修改单个竞猜记录
func (b *BetController) ActionEdit(args []byte) error {
	user := AuthUserToLogin(b.c)
	if user == nil || user.Manage != 1 {
		b.c.Redirect(http.StatusFound, "/404")
		return nil
	}
	var params struct {
		Id       int   `json:"id"`
		BetType  int   `json:"bet_type"`
		BetPlace int64 `json:"bet_place"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}

	model := models.NewBetModel(nil)
	data, err := model.GetById(params.Id)
	if err != nil {
		return err
	}

	data.BetType = params.BetType
	data.BetPlace = params.BetPlace
	err = model.Save(data)
	if err != nil {
		return err
	}
	if data.ItemOver == 1 {
		itemModel := models.NewItemModel(nil)
		item, err := itemModel.GetById(data.ItemId)
		if err != nil {
			return err
		}
		vt := calcVictory(item)
		err = calcUserScore(vt, data, item, model)
	}
	return err
}

// ActionDelete 物理删除用户竞猜记录
func (b *BetController) ActionDelete(args []byte) error {
	user := AuthUserToLogin(b.c)
	if user == nil || user.Manage != 1 {
		b.c.Redirect(http.StatusFound, "/404")
		return nil
	}
	var params struct {
		Id int `json:"id"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}

	model := models.NewBetModel(nil)
	return model.DeleteStruct(&models.BetData{Id: params.Id})
}
