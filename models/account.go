package models

import (
	"cc_template/common"

	"github.com/asdine/storm/v3"
)

// 管理用户数据
type AccountData struct {
	Id           int     `storm:"id,increment" json:"id"` //主键,自增长
	Name         string  `storm:"index" json:"name"`      //用户名
	Passwd       string  `json:"passwd"`                  //密码，默认密码都是1230123
	Manage       int     `json:"manage"`                  //是否管理员
	Init         int     `json:"init"`                    //是否初始化 0，1，如果为0强制修改密码
	VictoryPlace float64 `json:"victory_place"`           //本场胜负金额
	CreatedDate  int64   `json:"created_date"`            //创建时间
	ModifiedDate int64   `json:"modified_date"`           //修改时间
}

// AccountModel 表名
type AccountModel struct {
	Table string `json:"table"` //表名
	CommonModel[AccountData]
}

func NewAccountModel(db *storm.DB) *AccountModel {
	if db == nil {
		db = common.BDB
	}

	return &AccountModel{
		Table: "account",
		CommonModel: CommonModel[AccountData]{
			Order: "DESC",
			Node:  db.From("account"),
		},
	}
}

func (u *AccountModel) GetByName(name string) (*AccountData, error) {
	data := new(AccountData)
	err := u.One("Name", name, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
