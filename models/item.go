package models

import (
	"cc_template/common"
	"github.com/asdine/storm"
)

var ItemConditionList = []*ConditionData{
	{
		Name:  "平手",
		Value: "0",
	},
	{
		Name:  "平手/半球",
		Value: "0|0.5",
	},
	{
		Name:  "半球",
		Value: "0.5",
	},
	{
		Name:  "半球/一球",
		Value: "0.5|1",
	},
	{
		Name:  "一球",
		Value: "1",
	},
	{
		Name:  "一球/球半",
		Value: "1|1.5",
	},
	{
		Name:  "球半",
		Value: "1.5",
	},
	{
		Name:  "球半/二球",
		Value: "1.5|2",
	},
	{
		Name:  "二球",
		Value: "2",
	},
	{
		Name:  "二球/二球半",
		Value: "2|2.5",
	},
	{
		Name:  "二球半",
		Value: "2.5",
	},
	{
		Name:  "二球半/三球",
		Value: "2.5|3",
	},
	{
		Name:  "三球",
		Value: "3",
	},
}

var ItemConditionMap = map[string]string{
	"0":     "平手",
	"0|0.5": "平手/半球",
	"0.5":   "半球",
	"0.5|1": "半球/一球",
	"1":     "一球",
	"1|1.5": "一球/球半",
	"1.5":   "球半",
	"1.5|2": "球半/二球",
	"2":     "二球",
	"2|2.5": "二球/二球半",
	"2.5":   "二球半",
	"2.5|3": "二球半/三球",
	"3":     "三球",
}

type ConditionData struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ItemData 项目数据
type ItemData struct {
	Id           int     `storm:"id,increment" json:"id"`      //主键,自增长
	RightName    string  `json:"right_name"`                   //主场名称
	LeftName     string  `json:"left_name"`                    //客场名称
	RightScore   int     `json:"right_score,string"`           //主场得分
	LeftScore    int     `json:"left_score,string"`            //客场得分
	VictoryCond  string  `json:"victory_cond"`                 //主场胜负条件
	RightOdds    float64 `json:"right_odds,string"`            //主场赔率
	LeftOdds     float64 `json:"left_odds,string"`             //客场赔率
	Victory      int     `json:"victory"`                      //最终胜利结果， 0，1，2 0平手，1主队，2客队
	IsOver       int     `storm:"index" json:"is_over,string"` //是否结束，0，1 是为结束
	VictoryPlace float64 `json:"victory_place"`                //本场胜负金额
	MinScore     int     `json:"min_score,string"`             //最小投积分
	MaxScore     int     `json:"max_score,string"`             //最大投积分
	GameTime     int64   `json:"game_time"`                    //开赛时间
	CreatedDate  int64   `json:"created_date"`                 //创建时间
	ModifiedDate int64   `json:"modified_date"`                //修改时间
}

// ItemModel 表名
type ItemModel struct {
	Table string `json:"table"` //表名
	CommonModel[ItemData]
}

func NewItemModel(db *storm.DB) *ItemModel {
	if db == nil {
		db = common.BDB
	}

	return &ItemModel{
		Table: "item",
		CommonModel: CommonModel[ItemData]{
			Order: "DESC",
			Node:  db.From("item"),
		},
	}
}
