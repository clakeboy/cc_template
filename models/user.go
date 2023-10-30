package models

import (
	"cc_template/common"

	"github.com/asdine/storm/v3"
)

// UserData 用户数据
type UserData struct {
	Id           int     `storm:"id,increment" json:"id"` //主键,自增长
	Name         string  `storm:"index" json:"name"`      //用户名
	Passwd       string  `json:"passwd"`                  //密码，默认密码都是1230123
	Manage       int     `json:"manage"`                  //是否管理员
	Init         int     `json:"init"`                    //是否初始化 0，1，如果为0强制修改密码
	VictoryPlace float64 `json:"victory_place"`           //本场胜负金额
	CreatedDate  int64   `json:"created_date"`            //创建时间
	ModifiedDate int64   `json:"modified_date"`           //修改时间
}

// UserModel 表名
type UserModel struct {
	Table string `json:"table"` //表名
	CommonModel[UserData]
}

func NewUserModel(db *storm.DB) *UserModel {
	if db == nil {
		db = common.BDB
	}

	return &UserModel{
		Table: "user",
		CommonModel: CommonModel[UserData]{
			Order: "DESC",
			Node:  db.From("user"),
		},
	}
}

func (u *UserModel) GetByName(name string) (*UserData, error) {
	data := new(UserData)
	err := u.One("Name", name, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
