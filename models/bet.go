package models

import (
	"cc_template/common"
	"github.com/asdine/storm"
	"github.com/asdine/storm/q"
)

type BetData struct {
	Id            int     `storm:"id,increment" json:"id"` //主键,自增长
	ItemId        int     `storm:"index" json:"item_id"`   //项目ID
	ItemRightName string  `json:"item_right_name"`         //主队名称
	ItemLeftName  string  `json:"item_left_name"`          //客队名称
	ItemVictory   int     `json:"item_victory"`            //最终结果 计算主队胜利 1为胜利，2为输半，3为输，0为平手
	ItemScore     string  `json:"item_score"`              //最终比分
	ItemOver      int     `json:"item_over"`               //是否结束， 1为结束
	ItemGameTime  int64   `json:"item_game_time"`          //开赛时间
	UserId        int     `storm:"index" json:"user_id"`   //用户ID
	UserName      string  `json:"user_name"`               //用户名
	BetType       int     `json:"bet_type"`                //下注类型，1为主队，2为客队
	BetPlace      int64   `json:"bet_place"`               //下注金额
	VictoryPlace  float64 `json:"victory_place"`           //胜负金额
	ModifiedDate  int64   `json:"modified_date"`           //修改时间
	CreatedDate   int64   `json:"created_date"`            //创建时间
}

// BetModel 表名
type BetModel struct {
	Table string `json:"table"` //表名
	CommonModel[BetData]
}

func NewBetModel(db *storm.DB) *BetModel {
	if db == nil {
		db = common.BDB
	}

	return &BetModel{
		Table: "bet",
		CommonModel: CommonModel[BetData]{
			Order: "DESC",
			Node:  db.From("bet"),
		},
	}
}

func (b *BetModel) GetByItemUserId(userId, itemId int) (*BetData, error) {
	data := new(BetData)
	query := b.Select(q.And(q.Eq("UserId", userId), q.Eq("ItemId", itemId)))
	err := query.First(data)
	if err != nil {
		return nil, err
	}
	return data, err
}

func (b *BetModel) GetUserBetList(userId, page, number int) ([]*BetData, error) {
	list, err := b.List(page, number, q.Eq("UserId", userId))

	if err != nil {
		return nil, err
	}
	return list, nil
}

func (b *BetModel) GetItemBetList(itemId, page, number int) ([]*BetData, error) {
	list, err := b.List(page, number, q.Eq("ItemId", itemId))
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (b *BetModel) GetItemBetCount(itemId int) int {
	count, err := b.Select(q.Eq("ItemId", itemId)).Count(new(BetData))
	if err != nil {
		return 0
	}
	return count
}
