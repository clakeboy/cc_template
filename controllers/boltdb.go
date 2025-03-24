package controllers

import (
	"archive/zip"
	"bytes"
	"cc_template/common"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/asdine/storm/v3"
	"github.com/asdine/storm/v3/index"
	"github.com/clakeboy/golib/components"
	"github.com/clakeboy/golib/utils"
	"github.com/gin-gonic/gin"
	"go.etcd.io/bbolt"
)

var tmpDir = "./temp"
var exportLog = components.NewSysLog("db_export_")

type DatabaseData struct {
	Icon     string          `json:"icon"`     //图标
	Key      string          `json:"key"`      //唯一键
	Text     string          `json:"text"`     //文字
	Children []*DatabaseData `json:"children"` //子菜单
}

type Query struct {
	Field string `json:"field"`
	Type  string `json:"type"`
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

// 删除数据
func (b *BoltdbManageController) ActionDelete(args []byte) error {
	var params struct {
		Table  string `json:"table"`   //table
		IdList []int  `json:"id_list"` //数据ID
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}
	tables := strings.Split(params.Table, "|")
	err = common.BDB.Bolt.Update(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		for _, v := range tables {
			if b != nil {
				b = b.Bucket([]byte(v))
			} else {
				b = tx.Bucket([]byte(v))
			}
		}
		if b == nil {
			return fmt.Errorf("bucket not found")
		}

		for _, id := range params.IdList {
			k, err := toBytes(id)
			if err != nil {
				return fmt.Errorf("生成KEY出错: %v", err)
			}
			err = b.Delete(k)
			if err != nil {
				return fmt.Errorf("删除数据出错: %v", err)
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
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
	var count int64 = 0
	err := db.Bolt.View(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		for _, v := range name {
			if b != nil {
				b = b.Bucket([]byte(v))
			} else {
				b = tx.Bucket([]byte(v))
			}
		}

		// b = b.Bucket([]byte("__storm_metadata"))

		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil && !bytes.HasPrefix(k, []byte("__storm")); k, _ = c.Next() {
			count++
		}
		// b.ForEach(func(k, v []byte) error {
		// 	count++
		// 	return nil
		// })
		// c := b.Get([]byte("Idcounter"))
		// var err2 error
		// count, err2 = numberfromb(c)
		// return err2
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

func toBytes(key any) ([]byte, error) {
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

func numberfromb(raw []byte) (int64, error) {
	r := bytes.NewReader(raw)
	var to int64
	err := binary.Read(r, binary.BigEndian, &to)
	if err != nil {
		return 0, err
	}
	return to, nil
}

// 导出数据对像
type Export []*ExportTask

// 添加数据
func (e *Export) Add(item *ExportTask) {
	bs := *e
	if len(bs) > 10 {
		rmItem := bs[0]
		bs = append(bs[1:], item)
		os.Remove(rmItem.FilePath)
	} else {
		bs = append(bs, item)
	}
	*e = bs
}

// 输出JSON数据
func (e Export) JSON() ([]byte, error) {
	return json.Marshal(e)
}

// JSON数据导入
func (e Export) ParseJson(data []byte) error {
	return json.Unmarshal(data, &e)
}

type ExportTask struct {
	Name          string  `json:"name"`           //任务名称
	Table         string  `json:"table"`          //任务表名
	Query         []Query `json:"query"`          //参数
	Status        string  `json:"status"`         //状态
	FilePath      string  `json:"file_path"`      //导出文件位置
	CreatedDate   int64   `json:"created_date"`   //创建时间
	CompeleteDate int64   `json:"compelete_date"` //完成时间
}

// 导出数据为JSON
func (b *BoltdbManageController) ActionExport(args []byte) ([]byte, error) {
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

	go export(params.Table, params.Query, params.Page, params.Number)

	return nil, nil
}

// 执行导出任务
func export(table string, query []Query, page, number int) {
	taskItem := &ExportTask{
		Name:          "导出数据表:" + table,
		Table:         table,
		Query:         query,
		Status:        "开始导出",
		CreatedDate:   time.Now().Unix(),
		CompeleteDate: 0,
	}

	var exportList Export

	err := common.BDB.Get("export", "task", &exportList)
	if err != nil && err != storm.ErrNotFound {
		exportLog.Error(fmt.Errorf("打开导出任务数据列表出错：%v", err))
		return
	}
	if err == storm.ErrNotFound {
		exportList = Export{}
	}
	exportList.Add(taskItem)
	err = common.BDB.Set("export", "task", exportList)
	if err != nil {
		exportLog.Error(fmt.Errorf("任务数据写入出错: %v", err))
		return
	}

	tableArr := strings.Split(table, "|")

	tableName := tableArr[len(tableArr)-1]
	list, err := getTableData(common.BDB, tableArr, (page-1)*number, number, query)
	if err != nil {
		taskItem.Status = fmt.Sprintf("打开导出数据列表出错：%v", err)
		common.BDB.Set("export", "task", exportList)
		exportLog.Error(fmt.Errorf("打开导出数据列表出错：%v", err))
		return
	}
	buf := new(bytes.Buffer)
	zipFile := zip.NewWriter(buf)
	defer zipFile.Close()
	jsonFile, err := zipFile.Create(table + ".json")
	if err != nil {
		taskItem.Status = fmt.Sprintf("创建ZIP文件出错: %v", err)
		common.BDB.Set("export", "task", exportList)
		exportLog.Error(fmt.Errorf("创建ZIP文件出错: %v", err))
		return
	}
	data, err := json.Marshal(list)
	if err != nil {
		taskItem.Status = fmt.Sprintf("创建ZIP文件出错: %v", err)
		common.BDB.Set("export", "task", exportList)
		exportLog.Error(fmt.Errorf("创建ZIP文件出错: %v", err))
		return
	}
	jsonFile.Write(data)
	zipFile.Close()
	//写入文件
	if !utils.PathExists(tmpDir) {
		os.MkdirAll(tmpDir, 0755)
	}
	exportFile := fmt.Sprintf("%s/%s_%s.zip", tmpDir, tableName, time.Now().Format("20060102150405"))
	err = os.WriteFile(exportFile, buf.Bytes(), 0755)
	if err != nil {
		taskItem.Status = fmt.Sprintf("写入ZIP文件出错: %v", err)
		common.BDB.Set("export", "task", exportList)
		exportLog.Error(fmt.Errorf("写入ZIP文件出错: %v", err))
		return
	}
	taskItem.Status = "导出完成"
	taskItem.FilePath = exportFile
	taskItem.CompeleteDate = time.Now().Unix()
	err = common.BDB.Set("export", "task", exportList)
	if err != nil {
		exportLog.Error(fmt.Errorf("任务数据写入出错: %v", err))
	}
}

// 下载导出文件
func (b *BoltdbManageController) ActionDown() {
	tmp, ok := b.c.GetPostForm("f")
	if !ok {
		b.c.String(http.StatusNotFound, "")
		return
	}
	fullPath := fmt.Sprintf("%s/%s", tmpDir, path.Base(tmp))
	if !utils.PathExists(fullPath) {
		b.c.JSON(http.StatusNotFound, utils.M{
			"code": 1,
			"msg":  "文件不存在",
		})
		return
	}

	b.c.FileAttachment(fullPath, path.Base(fullPath))
}

// 得到导出任务列表
func (b *BoltdbManageController) ActionTaskList(args []byte) (Export, error) {
	var exportList Export
	err := common.BDB.Get("export", "task", &exportList)
	if err != nil && err != storm.ErrNotFound {
		return nil, fmt.Errorf("打开导出任务数据列表出错：%v", err)
	}
	slices.Reverse(exportList)
	return exportList, nil
}

// 数据导入
func (b *BoltdbManageController) ActionImport(args []byte) error {
	var params struct {
		Table []string  `json:"table"` //表名
		Data  []utils.M `json:"data"`  //数据列表
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}

	err = common.BDB.Bolt.Update(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		for _, v := range params.Table {
			if b != nil {
				b, err = b.CreateBucketIfNotExists([]byte(v))
				// b = b.Bucket([]byte(v))
			} else {
				b, err = tx.CreateBucketIfNotExists([]byte(v))
				// b = tx.Bucket([]byte(v))
			}
			if err != nil {
				return fmt.Errorf("创建bucket出错: %v", err)
			}
		}
		if b == nil {
			return fmt.Errorf("bucket not found")
		}

		for _, item := range params.Data {
			k, err := toBytes(item["id"])
			if err != nil {
				return fmt.Errorf("生成KEY出错: %v", err)
			}
			err = b.Put(k, item.ToJson())
			if err != nil {
				return fmt.Errorf("导入数据出错: %v", err)
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("导入数据出错: %v", err)
	}

	return nil
}

// 上传zip文件导入
func (b *BoltdbManageController) ActionUploadImport() error {
	file, err := b.c.FormFile("file")
	if err != nil {
		return fmt.Errorf("获取上传文件出错: %v", err)
	}
	f, err := file.Open()
	if err != nil {
		return fmt.Errorf("打开上传文件出错: %v", err)
	}

	taskItem := &ExportTask{
		Name:          "导入数据表:" + file.Filename,
		Table:         "",
		Query:         nil,
		Status:        "开始导入",
		CreatedDate:   time.Now().Unix(),
		CompeleteDate: 0,
	}

	taskList, err := getTaskList()
	if err != nil {
		return fmt.Errorf("打开导出任务数据列表出错：%v", err)
	}

	taskList.Add(taskItem)
	saveTaskList(taskList)
	go importData(f, file.Size, taskItem, taskList)
	return nil
}

func getTaskList() (Export, error) {
	var exportList Export
	err := common.BDB.Get("export", "task", &exportList)
	if err != nil && err != storm.ErrNotFound {
		return nil, fmt.Errorf("打开导出任务数据列表出错：%v", err)
	}
	if err == storm.ErrNotFound {
		exportList = Export{}
	}
	return exportList, nil
}

func saveTaskList(task Export) error {
	return common.BDB.Set("export", "task", task)
}

// 执行导入任务
func importData(f multipart.File, size int64, taskInfo *ExportTask, task Export) {
	// zipReader, err := zip.NewReader(f, size)
	// if err != nil {
	// 	exportLog.Error(fmt.Errorf("打开ZIP文件出错: %v", err))
	// 	return
	// }
	// for _, zf := range zipReader.File {
	// 	name := path.Base(zf.Name)
	// }
}

// 下载并备份数据库文件
func (b *BoltdbManageController) ActionBackupDownload() {
	pathStr, name := path.Split(common.Conf.BDB.Path)
	backPath := path.Join(pathStr, "backup", time.Now().Format("20060102"))
	if !utils.PathExists(backPath) {
		err := os.MkdirAll(backPath, 0755)
		if err != nil {
			exportLog.Error(fmt.Errorf("创建备份目录出错: %v", err))
			return
		}
	}
	zipFileName := path.Join(backPath, fmt.Sprintf("%s.zip", time.Now().Format("20060102150405")))
	f, err := os.OpenFile(zipFileName, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		exportLog.Error(fmt.Errorf("打开ZIP文件出错: %v", err))
		return
	}
	defer f.Close()
	zipFile := zip.NewWriter(f)
	defer zipFile.Close()

	zf, err := zipFile.Create(name)
	if err != nil {
		exportLog.Error(fmt.Errorf("创建ZIP文件出错: %v", err))
		return
	}
	dbFile, err := os.Open(common.Conf.BDB.Path)
	if err != nil {
		exportLog.Error(fmt.Errorf("打开数据库文件出错: %v", err))
		return
	}
	defer dbFile.Close()
	_, err = io.Copy(zf, dbFile)
	if err != nil {
		exportLog.Error(fmt.Errorf("写入ZIP文件出错: %v", err))
		return
	}
	zipFile.Close()
	b.c.FileAttachment(zipFileName, path.Base(zipFileName))
}
