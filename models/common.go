package models

import (
	"github.com/asdine/storm"
	"github.com/asdine/storm/q"
)

type CommonModel[T any] struct {
	Order string
	storm.Node
}

type QueryResult[T any] struct {
	List  []*T
	Count int
}

// SetOrder 设置排序方式
func (u *CommonModel[T]) SetOrder(ord string) {
	u.Order = ord
}

// GetById 通过ID拿到记录
func (u *CommonModel[T]) GetById(id int) (*T, error) {
	data := new(T)
	err := u.One("Id", id, data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Query 查询条件得到任务数据列表
func (u *CommonModel[T]) Query(page, number int, where ...q.Matcher) (*QueryResult[T], error) {
	var list []*T
	count, err := u.Select(where...).Count(new(T))
	if err != nil {
		return nil, err
	}
	query := u.Select(where...)
	if u.Order == "DESC" {
		query.Reverse()
	}
	err = query.Limit(number).Skip((page - 1) * number).Find(&list)
	if err != nil {
		return nil, err
	}
	return &QueryResult[T]{
		List:  list,
		Count: count,
	}, nil
}

// List 查询条件得到任务数据列表
func (u *CommonModel[T]) List(page, number int, where ...q.Matcher) ([]*T, error) {
	var list []*T
	query := u.Select(where...)
	if u.Order == "DESC" {
		query.Reverse()
	}
	err := query.Limit(number).Skip((page - 1) * number).Find(&list)
	if err != nil {
		return nil, err
	}
	return list, nil
}
