package controllers

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"cc_template/common"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/asdine/storm/v3"
	"github.com/asdine/storm/v3/index"
	"github.com/clakeboy/golib/utils"
	"github.com/gin-gonic/gin"
	"go.etcd.io/bbolt"
)

type DatabaseData struct {
	Icon     string          `json:"icon"`     //图标
	Key      string          `json:"key"`      //唯一键
	Text     string          `json:"text"`     //文字
	Children []*DatabaseData `json:"children"` //子菜单
}

type Query struct {
	Field string `json:"field"`
	TYpe  string `json:"type"`
	Value any    `json:"value"`
	Index bool   `json:"index"`
}

// Cursor that can be reversed
type Cursor struct {
	C       *bbolt.Cursor
	Reverse bool
}

// First element
func (c *Cursor) First() ([]byte, []byte) {
	if c.Reverse {
		return c.C.Last()
	}

	return c.C.First()
}

// Next element
func (c *Cursor) Next() ([]byte, []byte) {
	if c.Reverse {
		return c.C.Prev()
	}

	return c.C.Next()
}

// 数据头
type Header struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type HeaderList []Header

func (h HeaderList) Len() int {
	return len(h)
}

func (h HeaderList) Less(i, j int) bool {
	return strings.Compare(h[i].Name, h[j].Name) == -1
}

func (h HeaderList) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// 控制器
type BoltdbManageController struct {
	c *gin.Context
}

func NewBoltdbManageController(c *gin.Context) *BoltdbManageController {
	return &BoltdbManageController{c: c}
}

// 获取表数据
func (b *BoltdbManageController) ActionQuery(args []byte) (utils.M, error) {
	var params struct {
		Table  string  `json:"table"`  //table 名
		Page   int     `json:"page"`   //页
		Number int     `json:"number"` //记录数
		Query  []Query `json:"query"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}
	var header HeaderList
	var headList []string
	list, err := getTableData(common.BDB, strings.Split(params.Table, "|"), (params.Page-1)*params.Number, params.Number, params.Query)
	for _, row := range list {
		for k := range row {
			if !slices.Contains(headList, k) {
				headList = append(headList, k)
				header = append(header, Header{
					Name: k,
					Type: getDataType(row[k]),
				})
			}
		}
	}
	if err != nil {
		return nil, err
	}
	count, err := getTableCount(common.BDB, strings.Split(params.Table, "|"))
	if err != nil {
		count = 0
	}
	sort.Sort(header)
	return utils.M{
		"list":   list,
		"header": header,
		"count":  count,
	}, nil
}

// 保存修改数据
func (b *BoltdbManageController) ActionSave(args []byte) error {
	var params struct {
		Table string `json:"table"` //table
		Id    int    `json:"id"`    //数据ID
		Data  string `json:"data"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}

	key, err := toBytes(params.Id)
	if err != nil {
		return fmt.Errorf("数据ID转换错误: %v", err)
	}
	err = setTableData(common.BDB, strings.Split(params.Table, "|"), key, []byte(params.Data))
	return err
}

// 得到所有数据表列表
func (b *BoltdbManageController) ActionDatabases(args []byte) ([]*DatabaseData, error) {
	var params struct {
		Name string `json:"name"`
	}
	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}
	var res []string
	if params.Name == "" {
		res, err = getBoltdbList(common.BDB)
	} else {
		res, err = getChildData(common.BDB, params.Name)
	}
	if err != nil {
		return nil, err
	}
	var list []*DatabaseData
	for _, v := range res {
		var child []*DatabaseData
		childRes, _ := getChildData(common.BDB, v)
		for _, vv := range childRes {
			indexList, _ := getTableIndex(common.BDB, []string{v, vv})
			var indexChild []*DatabaseData
			for _, vvv := range indexList {
				indexChild = append(indexChild, &DatabaseData{
					Icon: "table",
					Key:  fmt.Sprintf("%s|%s|%s", v, vv, vvv),
					Text: vvv,
				})
			}
			child = append(child, &DatabaseData{
				Icon:     "table",
				Key:      fmt.Sprintf("%s|%s", v, vv),
				Text:     vv,
				Children: indexChild,
			})

		}
		list = append(list, &DatabaseData{
			Icon:     "database",
			Key:      v,
			Text:     v,
			Children: child,
		})
	}
	return list, nil
}

// 获取数据表列表
func getBoltdbList(db *storm.DB) ([]string, error) {
	var list []string
	db.Bolt.View(func(tx *bbolt.Tx) error {
		tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			list = append(list, string(name))
			return nil
		})
		return nil
	})
	return list, nil
}

func getChildData(db *storm.DB, name string) ([]string, error) {
	var list []string
	db.Bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(name))
		b.ForEach(func(k, v []byte) error {
			list = append(list, string(k))
			return nil
		})
		return nil
	})
	return list, nil
}

// 得到表数据
func getTableData(db *storm.DB, name []string, start, limit int, query []Query) ([]utils.M, error) {
	if len(name) == 0 {
		return nil, fmt.Errorf("name is empty")
	}

	var list []utils.M
	err := db.Bolt.View(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		for _, v := range name {
			if b != nil {
				b = b.Bucket([]byte(v))
			} else {
				b = tx.Bucket([]byte(v))
			}
		}
		if b == nil {
			return fmt.Errorf("bucket is nil")
		}
		c := &Cursor{C: b.Cursor(), Reverse: true}
		if len(query) > 0 && query[0].Index {
			rows, err := findData(b, query[0].Field, query[0].Value, &index.Options{
				Reverse: true,
				Skip:    start,
				Limit:   limit,
			})
			if err != nil {
				return err
			}
			for _, k := range rows {
				v := b.Get(k)
				ds := utils.M{}
				ds.ParseJson(v)
				list = append(list, ds)
			}
			return nil
		} else {
			idx := 0
			// prefix := utils.IntToBytes(start, 32)
			var find *Query
			if len(query) > 0 {
				find = &query[0]
			}
			for k, v := c.First(); k != nil; k, v = c.Next() {
				kidx := utils.BytesToInt(k)
				if kidx == 0 {
					continue
				}
				if len(list) >= limit {
					break
				}

				ds := utils.M{}
				ds.ParseJson(v)
				if find != nil {
					if ds[find.Field] != find.Value {
						continue
					}
				}
				if idx < start {
					idx++
					continue
				}
				list = append(list, ds)
				idx++
			}
		}

		return nil
	})
	return list, err
}

// 修改数据
func setTableData(db *storm.DB, tableName []string, key, val []byte) error {
	err := db.Bolt.Update(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		for _, v := range tableName {
			if b != nil {
				b = b.Bucket([]byte(v))
			} else {
				b = tx.Bucket([]byte(v))
			}
		}
		if b == nil {
			return fmt.Errorf("bucket not found")
		}

		if b.Get(key) == nil {
			return fmt.Errorf("key not found")
		}

		return b.Put(key, val)
	})
	return err
}

// 得到表记录数
func getTableCount(db *storm.DB, name []string) (int64, error) {
	var count int64
	err := db.Bolt.View(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		for _, v := range name {
			if b != nil {
				b = b.Bucket([]byte(v))
			} else {
				b = tx.Bucket([]byte(v))
			}
		}

		b = b.Bucket([]byte("__storm_metadata"))

		if b == nil {
			return fmt.Errorf("bucket not found")
		}

		c := b.Get([]byte("Idcounter"))
		count = int64(utils.BytesToInt(c))
		return nil
	})

	return count, err
}

// 得到索引列表
func getTableIndex(db *storm.DB, name []string) ([]string, error) {
	var list []string
	err := db.Bolt.View(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		for _, v := range name {
			if b != nil {
				b = b.Bucket([]byte(v))
			} else {
				b = tx.Bucket([]byte(v))
			}
		}

		if b == nil {
			return fmt.Errorf("bucket not found")
		}

		c := b.Cursor()
		prefix := []byte("__storm")
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			list = append(list, string(k))
		}

		return nil
	})

	return list, err
}

var indexPrefix = "__storm_index_"

// 查询字段
func findData(b *bbolt.Bucket, key string, val any, opts *index.Options) ([][]byte, error) {
	var idx index.Index
	var err error
	if key == "id" {
		idx, err = index.NewUniqueIndex(b, []byte(indexPrefix+utils.Under2Hump(key)))
	} else {
		idx, err = index.NewListIndex(b, []byte(indexPrefix+utils.Under2Hump(key)))
	}

	if err != nil {
		return nil, err
	}
	valBytes, err := toBytes(val)
	if err != nil {
		return nil, err
	}
	list, err := idx.All(valBytes, opts)
	if err != nil {
		return nil, err
	}
	return list, err
}

// 得到数据类型
func getDataType(data any) string {
	switch data.(type) {
	case string:
		return "string"
	case int, int64:
		return "int"
	case float64:
		return "float64"
	case bool:
		return "bool"
	case []byte:
		return "[]byte"
	case time.Time:
		return "time"
	default:
		return "none"
	}
}

func toBytes(key interface{}) ([]byte, error) {
	if key == nil {
		return nil, nil
	}
	switch t := key.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	case int:
		return numbertob(int64(t))
	case uint:
		return numbertob(uint64(t))
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return numbertob(t)
	case float64:
		return numbertob(int64(t))
	default:
		return json.Marshal(key)
	}
}

func numbertob(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
