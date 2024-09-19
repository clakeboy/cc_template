package models

import (
	"reflect"

	"github.com/asdine/storm/v3"
	"github.com/asdine/storm/v3/q"
	"github.com/clakeboy/golib/utils"
	"github.com/tidwall/gjson"
)

type CommonModel[T any] struct {
	Order      string
	OrderField string
	storm.Node
}

type QueryResult[T any] struct {
	List  []*T `json:"list"`
	Count int  `json:"count"`
}

// SetOrder 设置排序方式
func (u *CommonModel[T]) SetOrder(field string, ord string) {
	u.OrderField = field
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

	if u.OrderField != "" {
		query.OrderBy(u.OrderField)
	}
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
	if u.OrderField != "" {
		query.OrderBy(u.OrderField)
	}
	if u.Order == "DESC" {
		query.Reverse()
	}
	err := query.Limit(number).Skip((page - 1) * number).Find(&list)
	if err != nil {
		return nil, err
	}
	return list, nil
}

// 更新所有条件
func (u *CommonModel[T]) UpdateMany(data utils.M, where ...q.Matcher) error {
	tx, err := u.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := u.Select(where...)
	err = query.Each(new(T), func(i interface{}) error {
		for k, v := range data {
			err := tx.UpdateField(i, k, v)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return tx.Commit()
}

// 得到字段计算合
func (u *CommonModel[T]) GetSum(field string, where ...q.Matcher) (float64, error) {
	var count float64
	query := u.Select(where...)
	// query.Limit(500)
	err := query.Each(new(T), func(i interface{}) error {
		val := reflect.ValueOf(i).Elem().FieldByName(field)
		switch val.Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32:
			count += float64(val.Int())
		case reflect.Float64:
			count += val.Float()
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return count, nil
}

// 使用json字段名统计
func (u *CommonModel[T]) GetSumRaw(field string, where ...q.Matcher) (float64, error) {
	var count float64
	ref := reflect.TypeOf((*T)(nil)).Elem()
	query := u.Select(where...).Bucket(ref.Name())

	err := query.RawEach(func(k, v []byte) error {
		count += gjson.GetBytes(v, field).Num
		return nil
	})

	if err != nil {
		return 0, err
	}

	return count, nil
}
