package models

import (
	"cc_template/common"
	"slices"

	"github.com/clakeboy/storm-rev"
)

// 后台菜单功能
type MenuData struct {
	Id           int         `storm:"id,increment" json:"id"` //主键,自增长
	ParentId     int         `storm:"index" json:"parent_id"` //父ID
	ParentName   string      `json:"parent_name"`             //父ID
	Name         string      `storm:"index" json:"name"`      //菜单名
	Text         string      `storm:"index" json:"text"`      //菜单显示名
	Icon         string      `json:"icon"`                    //显示的icon
	Link         string      `json:"link"`                    //跳转链接
	Step         bool        `json:"step"`                    //是否节点显示
	Sort         int         `storm:"index" json:"sort"`      //显示顺序
	CreatedDate  int64       `json:"created_date"`            //创建日期
	CreatedBy    string      `json:"created_by"`              //创建人
	ModifiedDate int64       `json:"modified_date"`           //修改时间
	ModifiedBy   string      `json:"modified_by"`             //修改人
	Child        []*MenuData `json:"child"`                   //子菜单项，主要用于数据显示
}

// 表名
type MenuModel struct {
	Table string `json:"table"` //表名
	CommonModel[MenuData]
}

func NewMenuModel(db *storm.DB) *MenuModel {
	if db == nil {
		db = common.BDB
	}

	return &MenuModel{
		Table: "menu",
		CommonModel: CommonModel[MenuData]{
			Order: "DESC",
			Node:  db.From("menu"),
		},
	}
}

// 通过NAME拿到记录
func (m *MenuModel) GetByName(name string) (*MenuData, error) {
	data := new(MenuData)
	err := m.One("Name", name, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// 通过父ID拿到列表
func (m *MenuModel) GetByParentId(parentId int, filter []int) ([]*MenuData, error) {
	var data []*MenuData
	err := m.Find("ParentId", parentId, &data)
	if err != nil {
		return nil, err
	}
	if filter != nil {
		var result []*MenuData
		for _, v := range data {
			if slices.Contains(filter, v.Id) {
				result = append(result, v)
			}
		}
		return result, nil
	}
	return data, nil
}
