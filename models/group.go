package models

import (
	"cc_template/common"

	"github.com/asdine/storm/v3"
)

// 后台用户组
type GroupData struct {
	Id           int    `storm:"id,increment" json:"id"` //主键,自增长
	Name         string `storm:"index" json:"name"`      //组名称
	MenuList     []int  `json:"menu_list"`               //分组菜单项
	CreatedDate  int64  `json:"created_date"`            //创建时间
	CreatedBy    string `json:"created_by"`              //创建人
	ModifiedDate int64  `json:"modified_date"`           //修改时间
	ModifiedBy   string `json:"modified_by"`             //修改人
}

// 表名
type GroupModel struct {
	Table string `json:"table"` //表名
	CommonModel[GroupData]
}

func NewGroupModel(db *storm.DB) *GroupModel {
	if db == nil {
		db = common.BDB
	}

	return &GroupModel{
		Table: "group",
		CommonModel: CommonModel[GroupData]{
			Order: "DESC",
			Node:  db.From("group"),
		},
	}
}

// 通过NAME拿到记录
func (g *GroupModel) GetByName(name string) (*GroupData, error) {
	data := new(GroupData)
	err := g.One("Name", name, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
