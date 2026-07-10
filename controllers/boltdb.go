package controllers

import (
	"archive/zip"
	"bytes"
	"cc_template/common"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/clakeboy/golib/components"
	"github.com/clakeboy/golib/utils"
	"github.com/clakeboy/storm-rev"
	"github.com/clakeboy/storm-rev/index"
	"github.com/gin-gonic/gin"
	"go.etcd.io/bbolt"
)

var tmpDir = "./temp"
var exportLog = components.NewSysLog("db_export_")
var sqlIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var sqlCountRe = regexp.MustCompile(`(?i)^\s*select\s+count\s*\(\s*\*`)
var sqlLimitRe = regexp.MustCompile(`(?i)\blimit\b`)
var sqlCountSelectRe = regexp.MustCompile(`(?i)\bcount\s*\(`)
var sqlSelectToCountRe = regexp.MustCompile(`(?i)^\s*select\s+[\s\S]*?\s+from\b`)
var sqlRemoveLimitOffsetRe = regexp.MustCompile(`(?i)\b(limit|offset)\b[\s\S]*`)
var sqlRemoveOrderRe = regexp.MustCompile(`(?i)\border\s+by\b[\s\S]*`)

type DatabaseData struct {
	Icon     string          `json:"icon"`     //图标
	Key      string          `json:"key"`      //唯一键
	Text     string          `json:"text"`     //文字
	Children []*DatabaseData `json:"children"` //子菜单
	Type     string          `json:"type"`     //节点类型:schema_from/schema_table/legacy
	From     string          `json:"from"`     //Storm 根 bucket
	Table    string          `json:"table"`    //Storm 表名
	SQL      bool            `json:"sql"`      //是否支持 storm-rev SQL
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

type tableRef struct {
	From    string
	Table   string
	Parts   []string
	Schema  bool
	Columns []storm.ColumnInfo
}

type sqlExecResult struct {
	Kind         string     `json:"kind"`
	List         []utils.M  `json:"list,omitempty"`
	Header       HeaderList `json:"header,omitempty"`
	Count        int        `json:"count,omitempty"`
	RowsAffected int        `json:"rows_affected,omitempty"`
	LastInsertID any        `json:"last_insert_id,omitempty"`
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

type DbItem struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// 获取所选的数据库实例，默认返回全局 common.BDB
func (b *BoltdbManageController) getDB(dbName string) *storm.DB {
	return common.GetDB(dbName)
}

// 获取可用数据库文件列表
func (b *BoltdbManageController) ActionDbList(args []byte) ([]DbItem, error) {
	var list []DbItem
	if common.Conf.BDB.List != nil {
		for name, path := range common.Conf.BDB.List {
			list = append(list, DbItem{
				Name: name,
				Path: path,
			})
		}
	} else {
		// 回退方案：扫描本地数据库目录
		files, err := os.ReadDir("./db")
		if err == nil {
			for _, f := range files {
				if !f.IsDir() && strings.HasSuffix(f.Name(), ".db") {
					list = append(list, DbItem{
						Name: f.Name(),
						Path: filepath.Join("./db", f.Name()),
					})
				}
			}
		}
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})
	return list, nil
}

// 获取表数据
func (b *BoltdbManageController) ActionQuery(args []byte) (utils.M, error) {
	var params struct {
		Db     string  `json:"db"`
		Table  string  `json:"table"`  //table 名
		Page   int     `json:"page"`   //页
		Number int     `json:"number"` //记录数
		Query  []Query `json:"query"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}
	db := b.getDB(params.Db)
	list, header, count, err := queryTableData(db, strings.Split(params.Table, "|"), params.Page, params.Number, params.Query, true)
	if err != nil {
		return nil, err
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
		Db    string `json:"db"`
		Table string `json:"table"` //table
		Id    int    `json:"id"`    //数据ID
		Data  string `json:"data"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}

	db := b.getDB(params.Db)
	ref, err := resolveTableRef(db, strings.Split(params.Table, "|"))
	if err != nil {
		return err
	}
	if ref.Schema {
		return updateSchemaTableData(db, ref, params.Id, []byte(params.Data))
	}
	key, err := toBytes(params.Id)
	if err != nil {
		return fmt.Errorf("数据ID转换错误: %v", err)
	}
	return setTableData(db, ref.Parts, key, []byte(params.Data))
}

// 删除数据
func (b *BoltdbManageController) ActionDelete(args []byte) error {
	var params struct {
		Db     string `json:"db"`
		Table  string `json:"table"`   //table
		IdList []int  `json:"id_list"` //数据ID
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}
	tables := strings.Split(params.Table, "|")
	db := b.getDB(params.Db)
	ref, err := resolveTableRef(db, tables)
	if err != nil {
		return err
	}
	if ref.Schema {
		return deleteSchemaTableData(db, ref, params.IdList)
	}
	err = db.Bolt.Update(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		for _, v := range ref.Parts {
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

// 删除整表
func (b *BoltdbManageController) ActionDeleteBucket(args []byte) error {
	var params struct {
		Db   string `json:"db"`
		Name string `json:"name"`
	}
	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}

	db := b.getDB(params.Db)
	ref, err := resolveTableRef(db, strings.Split(params.Name, "|"))
	if err != nil {
		return err
	}
	if ref.Schema {
		return dropSchemaTable(db, ref)
	}
	return deleteRawBucket(db, ref.Parts)
}

// 得到所有数据表列表
func (b *BoltdbManageController) ActionDatabases(args []byte) ([]*DatabaseData, error) {
	var params struct {
		Db   string `json:"db"`
		Name string `json:"name"`
	}
	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}
	db := b.getDB(params.Db)
	return getDatabaseTree(db)
}

// ActionSqlExec 通过 storm-rev SQL 层执行单表 SQL。
// SELECT/COUNT 返回表格数据；INSERT/UPDATE/DELETE 返回影响行数。
func (b *BoltdbManageController) ActionSqlExec(args []byte) (*sqlExecResult, error) {
	var params struct {
		Db     string `json:"db"`
		From   string `json:"from"`
		SQL    string `json:"sql"`
		Args   []any  `json:"args"`
		Page   int    `json:"page"`
		Number int    `json:"number"`
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return nil, err
	}
	db := b.getDB(params.Db)
	sqlText := strings.TrimSpace(params.SQL)
	if sqlText == "" {
		return nil, fmt.Errorf("SQL 不能为空")
	}
	manager, err := sqlManager(db, params.From)
	if err != nil {
		return nil, err
	}
	kind := sqlKind(sqlText)
	switch kind {
	case "count":
		count, err := manager.Count(sqlText, params.Args...)
		if err != nil {
			return nil, normalizeSQLError(err)
		}
		return &sqlExecResult{
			Kind:   "count",
			List:   []utils.M{{"count": count}},
			Header: HeaderList{{Name: "count", Type: "int"}},
			Count:  count,
		}, nil
	case "select":
		// 判断用户是否主动传入了 limit，或者语句是 count 语句
		hasLimit := sqlLimitRe.MatchString(sqlText)
		isCountSelect := sqlCountSelectRe.MatchString(sqlText)

		var limitVal, offsetVal int
		if !hasLimit && !isCountSelect {
			// 如果没有主动传 limit 和 offset，则记录并拼接限制值
			hasSemi := strings.HasSuffix(sqlText, ";")
			if hasSemi {
				sqlText = strings.TrimSuffix(sqlText, ";")
			}
			if params.Page > 0 && params.Number > 0 {
				limitVal = params.Number
				offsetVal = (params.Page - 1) * params.Number
				sqlText = fmt.Sprintf("%s LIMIT %d OFFSET %d", sqlText, limitVal, offsetVal)
			} else {
				limitVal = 100
				offsetVal = 0
				sqlText = sqlText + " LIMIT 100"
			}
			if hasSemi {
				sqlText = sqlText + ";"
			}
		}

		var rows []map[string]any
		if err := manager.Project(sqlText, &rows, params.Args...); err != nil {
			return nil, normalizeSQLError(err)
		}
		list := mapRowsToUtils(rows)

		totalCount := 0
		if hasLimit || isCountSelect {
			totalCount = len(list)
		} else {
			// 如果返回的数据行数少于限制大小，说明已经是最后一页，可直接计算得出总量，不需再进行数据库的 Count
			if len(list) < limitVal {
				totalCount = offsetVal + len(list)
			} else {
				// 否则仍需进行全量 Count 统计
				countSqlText := strings.TrimSpace(params.SQL)
				countSqlText = sqlRemoveLimitOffsetRe.ReplaceAllString(countSqlText, "")
				countSqlText = sqlRemoveOrderRe.ReplaceAllString(countSqlText, "")
				countSqlText = sqlSelectToCountRe.ReplaceAllString(countSqlText, "SELECT COUNT(*) FROM")
				if count, err := manager.Count(countSqlText, params.Args...); err == nil {
					totalCount = count
				}
			}
		}

		if totalCount == 0 || totalCount < len(list) {
			totalCount = len(list)
		}
		return &sqlExecResult{
			Kind:   "select",
			List:   list,
			Header: headersFromRows(list, nil),
			Count:  totalCount,
		}, nil
	case "exec":
		result, err := manager.Exec(sqlText, params.Args...)
		if err != nil {
			return nil, normalizeSQLError(err)
		}
		return &sqlExecResult{
			Kind:         "exec",
			RowsAffected: result.RowsAffected,
			LastInsertID: result.LastInsertID,
		}, nil
	default:
		return nil, fmt.Errorf("只支持 SELECT/INSERT/UPDATE/DELETE SQL")
	}
}

// getDatabaseTree 优先使用新版 storm-rev 的 schema 列表接口。
// 没有 schema 的旧 bucket 会继续按 Bolt 原始 bucket 显示，避免旧数据在界面消失。
func getDatabaseTree(db *storm.DB) ([]*DatabaseData, error) {
	list, seen, nodeByKey := getSchemaDatabaseTree(db)
	if err := appendLegacyDatabaseTree(db, &list, seen, nodeByKey); err != nil {
		return nil, err
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Text < list[j].Text
	})
	return list, nil
}

// getSchemaDatabaseTree 读取有 storm-rev schema 元数据的 From/Table。
// 这类表可以安全使用 SQL 查询和写入，因为字段、ID、索引信息都来自库元数据。
func getSchemaDatabaseTree(db *storm.DB) ([]*DatabaseData, map[string]bool, map[string]*DatabaseData) {
	seen := make(map[string]bool)
	nodeByKey := make(map[string]*DatabaseData)
	var list []*DatabaseData

	froms, err := db.ListFroms()
	if err != nil {
		return list, seen, nodeByKey
	}
	sort.Strings(froms)
	for _, from := range froms {
		tables, err := db.ListTables(from)
		if err != nil {
			continue
		}
		sort.Strings(tables)
		fromNode := &DatabaseData{
			Icon: "database",
			Key:  from,
			Text: from,
			Type: "schema_from",
			From: from,
			SQL:  true,
		}
		for _, table := range tables {
			columns, err := db.ListColumns(from, table)
			if err != nil {
				continue
			}
			tableNode := schemaTableNode(from, table, columns)
			fromNode.Children = append(fromNode.Children, tableNode)
			seen[tableNode.Key] = true
			nodeByKey[tableNode.Key] = tableNode
		}
		if len(fromNode.Children) > 0 {
			list = append(list, fromNode)
			seen[from] = true
			nodeByKey[from] = fromNode
		}
	}
	return list, seen, nodeByKey
}

func schemaTableNode(from, table string, columns []storm.ColumnInfo) *DatabaseData {
	node := &DatabaseData{
		Icon:  "table",
		Key:   joinTableKey(from, table),
		Text:  table,
		Type:  "schema_table",
		From:  from,
		Table: table,
		SQL:   true,
	}
	for _, column := range columns {
		if column.Index == "" && !column.ID {
			continue
		}
		indexName := "__storm_index_" + column.Name
		node.Children = append(node.Children, &DatabaseData{
			Icon:  "table",
			Key:   joinTableKey(from, table, indexName),
			Text:  indexName,
			Type:  "schema_index",
			From:  from,
			Table: table,
			SQL:   false,
		})
	}
	return node
}

// appendLegacyDatabaseTree 只补充新版 schema API 看不到的旧 bucket。
// 旧 bucket 仍使用原始 Bolt 读写，不承诺 SQL 和外部索引能力。
func appendLegacyDatabaseTree(db *storm.DB, list *[]*DatabaseData, seen map[string]bool, nodeByKey map[string]*DatabaseData) error {
	roots, err := getBoltdbList(db)
	if err != nil {
		return err
	}
	sort.Strings(roots)
	for _, root := range roots {
		if strings.HasPrefix(root, "__storm") {
			continue
		}
		children, _ := getRawBucketChildren(db, root)
		if len(children) == 0 {
			if seen[root] {
				continue
			}
			node := &DatabaseData{
				Icon: "table",
				Key:  root,
				Text: root,
				Type: "legacy",
				SQL:  false,
			}
			*list = append(*list, node)
			seen[root] = true
			nodeByKey[root] = node
			continue
		}

		parent := nodeByKey[root]
		if parent == nil {
			parent = &DatabaseData{
				Icon: "database",
				Key:  root,
				Text: root,
				Type: "legacy",
				From: root,
				SQL:  false,
			}
			*list = append(*list, parent)
			seen[root] = true
			nodeByKey[root] = parent
		}
		for _, child := range children {
			key := joinTableKey(root, child)
			if seen[key] {
				continue
			}
			parent.Children = append(parent.Children, &DatabaseData{
				Icon:  "table",
				Key:   key,
				Text:  child,
				Type:  "legacy",
				From:  root,
				Table: child,
				SQL:   false,
			})
			seen[key] = true
		}
	}
	return nil
}

func joinTableKey(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			clean = append(clean, part)
		}
	}
	return strings.Join(clean, "|")
}

// resolveTableRef 判定一个前端 table key 是否是有 schema 的 storm-rev 表。
// 只有 ListColumns 成功时才走 SQL 路径；否则保留 legacy raw Bolt 路径。
func resolveTableRef(db *storm.DB, parts []string) (tableRef, error) {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			clean = append(clean, part)
		}
	}
	if len(clean) == 0 {
		return tableRef{}, fmt.Errorf("table is empty")
	}
	if len(clean) >= 2 {
		columns, err := db.ListColumns(clean[0], clean[1])
		if err == nil {
			return tableRef{
				From:    clean[0],
				Table:   clean[1],
				Parts:   clean[:2],
				Schema:  true,
				Columns: columns,
			}, nil
		}
	}
	if len(clean) == 1 {
		columns, err := db.ListColumns("", clean[0])
		if err == nil {
			return tableRef{
				Table:   clean[0],
				Parts:   clean[:1],
				Schema:  true,
				Columns: columns,
			}, nil
		}
	}
	return tableRef{Parts: clean}, nil
}

func queryTableData(db *storm.DB, parts []string, page, number int, query []Query, reverse bool) ([]utils.M, HeaderList, int, error) {
	if page <= 0 {
		page = 1
	}
	if number <= 0 {
		number = 50
	}
	ref, err := resolveTableRef(db, parts)
	if err != nil {
		return nil, nil, 0, err
	}
	if ref.Schema {
		return querySchemaTableData(db, ref, (page-1)*number, number, query, reverse)
	}
	list, err := getTableData(db, ref.Parts, (page-1)*number, number, query, reverse)
	if err != nil {
		return nil, nil, 0, err
	}
	count, err := getTableCount(db, ref.Parts)
	if err != nil {
		count = 0
	}
	return list, headersFromRows(list, nil), int(count), nil
}

// querySchemaTableData 使用 storm-rev SQL.Project/Count 查询 schema 表。
// WHERE 条件只从已知列生成，值全部走参数绑定，避免把前端输入拼进 SQL。
func querySchemaTableData(db *storm.DB, ref tableRef, start, limit int, query []Query, reverse bool) ([]utils.M, HeaderList, int, error) {
	manager, err := sqlManager(db, ref.From)
	if err != nil {
		return nil, nil, 0, err
	}
	tableName, err := sqlIdent(ref.Table)
	if err != nil {
		return nil, nil, 0, err
	}
	where, args, err := buildSQLWhere(ref, query)
	if err != nil {
		return nil, nil, 0, err
	}
	order := ""
	if idName := idColumnName(ref.Columns); idName != "" {
		orderIdent, err := sqlIdent(idName)
		if err != nil {
			return nil, nil, 0, err
		}
		if reverse {
			order = " ORDER BY " + orderIdent + " DESC"
		} else {
			order = " ORDER BY " + orderIdent + " ASC"
		}
	}

	count, err := manager.Count("SELECT COUNT(*) FROM "+tableName+where, args...)
	if err != nil {
		return nil, nil, 0, err
	}

	var rows []map[string]any
	selectArgs := append(append([]any{}, args...), limit, start)
	sqlText := "SELECT * FROM " + tableName + where + order + " LIMIT ? OFFSET ?"
	if err := manager.Project(sqlText, &rows, selectArgs...); err != nil {
		return nil, nil, 0, err
	}
	return mapRowsToUtils(rows), schemaHeaders(ref.Columns), count, nil
}

func buildSQLWhere(ref tableRef, query []Query) (string, []any, error) {
	if len(query) == 0 {
		return "", nil, nil
	}
	cond := query[0]
	if cond.Field == "" {
		return "", nil, nil
	}
	if cond.Type != "" && cond.Type != "eq" {
		return "", nil, fmt.Errorf("暂只支持等值查询")
	}
	column, ok := findColumn(ref.Columns, cond.Field)
	if !ok {
		return "", nil, fmt.Errorf("未知字段: %s", cond.Field)
	}
	fieldName, err := sqlIdent(columnSQLName(column))
	if err != nil {
		return "", nil, err
	}
	return " WHERE " + fieldName + " = ?", []any{cond.Value}, nil
}

func updateSchemaTableData(db *storm.DB, ref tableRef, id int, raw []byte) error {
	var row map[string]any
	if err := json.Unmarshal(raw, &row); err != nil {
		return fmt.Errorf("错误的JSON数据结构: %v", err)
	}
	idColumn := idColumn(ref.Columns)
	if idColumn.Name == "" {
		return fmt.Errorf("schema 表缺少 ID 字段")
	}
	tableName, err := sqlIdent(ref.Table)
	if err != nil {
		return err
	}
	idName, err := sqlIdent(columnSQLName(idColumn))
	if err != nil {
		return err
	}
	var sets []string
	var args []any
	for _, column := range ref.Columns {
		if column.ID {
			continue
		}
		value, ok := row[column.JSON]
		if !ok {
			value, ok = row[column.Name]
		}
		if !ok {
			continue
		}
		name, err := sqlIdent(columnSQLName(column))
		if err != nil {
			return err
		}
		sets = append(sets, name+" = ?")
		args = append(args, value)
	}
	if len(sets) == 0 {
		return fmt.Errorf("没有可更新字段")
	}
	args = append(args, id)
	manager, err := sqlManager(db, ref.From)
	if err != nil {
		return err
	}
	result, err := manager.Exec("UPDATE "+tableName+" SET "+strings.Join(sets, ", ")+" WHERE "+idName+" = ?", args...)
	if err != nil {
		return err
	}
	if result.RowsAffected == 0 {
		return storm.ErrNotFound
	}
	return nil
}

func deleteSchemaTableData(db *storm.DB, ref tableRef, ids []int) error {
	if len(ids) == 0 {
		return nil
	}
	idColumn := idColumn(ref.Columns)
	if idColumn.Name == "" {
		return fmt.Errorf("schema 表缺少 ID 字段")
	}
	tableName, err := sqlIdent(ref.Table)
	if err != nil {
		return err
	}
	idName, err := sqlIdent(columnSQLName(idColumn))
	if err != nil {
		return err
	}
	holders := make([]string, 0, len(ids))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		holders = append(holders, "?")
		args = append(args, id)
	}
	manager, err := sqlManager(db, ref.From)
	if err != nil {
		return err
	}
	_, err = manager.Exec("DELETE FROM "+tableName+" WHERE "+idName+" IN ("+strings.Join(holders, ", ")+")", args...)
	return err
}

func dropSchemaTable(db *storm.DB, ref tableRef) error {
	if ref.From == "" {
		return db.Drop(ref.Table)
	}
	return db.From(ref.From).Drop(ref.Table)
}

func importSchemaTableData(db *storm.DB, ref tableRef, data []utils.M) error {
	if len(data) == 0 {
		return nil
	}
	tableName, err := sqlIdent(ref.Table)
	if err != nil {
		return err
	}
	columns := insertColumns(ref.Columns, data[0])
	if len(columns) == 0 {
		return fmt.Errorf("没有可导入字段")
	}
	var columnNames []string
	for _, column := range columns {
		name, err := sqlIdent(columnSQLName(column))
		if err != nil {
			return err
		}
		columnNames = append(columnNames, name)
	}

	var args []any
	rowHolders := make([]string, 0, len(data))
	oneRow := "(" + strings.TrimRight(strings.Repeat("?,", len(columns)), ",") + ")"
	for _, row := range data {
		rowHolders = append(rowHolders, oneRow)
		for _, column := range columns {
			value, ok := row[column.JSON]
			if !ok {
				value = row[column.Name]
			}
			args = append(args, value)
		}
	}
	manager, err := sqlManager(db, ref.From)
	if err != nil {
		return err
	}
	_, err = manager.Exec("INSERT INTO "+tableName+" ("+strings.Join(columnNames, ", ")+") VALUES "+strings.Join(rowHolders, ", "), args...)
	return err
}

func insertColumns(columns []storm.ColumnInfo, row utils.M) []storm.ColumnInfo {
	var result []storm.ColumnInfo
	for _, column := range columns {
		if _, ok := row[column.JSON]; ok {
			result = append(result, column)
			continue
		}
		if _, ok := row[column.Name]; ok {
			result = append(result, column)
		}
	}
	return result
}

func sqlManager(db *storm.DB, from string) (*storm.SQL, error) {
	// 管理端可能被其它项目复用，不能硬编码注册业务模型。
	// 新版 storm-rev SQL 会从 __storm_metadata/schema 懒加载表结构；
	// 没有 schema 的旧 bucket 保持 legacy raw 管理能力，不在这里伪造 SQL 模型。
	if from == "" {
		return db.SQL()
	}
	return db.From(from).SQL()
}

func normalizeSQLError(err error) error {
	// 缺少 schema 的 legacy 表无法通过 storm-rev SQL 层安全绑定字段、ID 和索引。
	// 这里把底层英文错误转成管理界面可读的原因，raw 表格查询仍由 legacy fallback 支持。
	if errors.Is(err, storm.ErrSQLTableNotRegistered) {
		return fmt.Errorf("当前表没有 storm-rev schema 元数据，不能使用 SQL 执行；请使用普通表格管理，或用新版 storm-rev 重新保存该表后再执行 SQL: %w", err)
	}
	return err
}

func sqlKind(sqlText string) string {
	lower := strings.ToLower(strings.TrimSpace(sqlText))
	switch {
	case sqlCountRe.MatchString(sqlText):
		return "count"
	case strings.HasPrefix(lower, "select"):
		return "select"
	case strings.HasPrefix(lower, "insert"), strings.HasPrefix(lower, "update"), strings.HasPrefix(lower, "delete"):
		return "exec"
	default:
		return ""
	}
}

func sqlIdent(name string) (string, error) {
	if !sqlIdentRe.MatchString(name) {
		return "", fmt.Errorf("非法 SQL 标识符: %s", name)
	}
	return name, nil
}

func findColumn(columns []storm.ColumnInfo, name string) (storm.ColumnInfo, bool) {
	for _, column := range columns {
		if strings.EqualFold(column.Name, name) || strings.EqualFold(column.JSON, name) {
			return column, true
		}
	}
	return storm.ColumnInfo{}, false
}

func idColumn(columns []storm.ColumnInfo) storm.ColumnInfo {
	for _, column := range columns {
		if column.ID {
			return column
		}
	}
	if column, ok := findColumn(columns, "id"); ok {
		return column
	}
	return storm.ColumnInfo{}
}

func idColumnName(columns []storm.ColumnInfo) string {
	column := idColumn(columns)
	if column.Name == "" {
		return ""
	}
	return columnSQLName(column)
}

func columnSQLName(column storm.ColumnInfo) string {
	if column.JSON != "" {
		return column.JSON
	}
	return column.Name
}

func schemaHeaders(columns []storm.ColumnInfo) HeaderList {
	headers := make(HeaderList, 0, len(columns))
	for _, column := range columns {
		headers = append(headers, Header{
			Name: columnSQLName(column),
			Type: schemaType(column.Type),
		})
	}
	sort.Sort(headers)
	return headers
}

func schemaType(name string) string {
	switch name {
	case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		return "int"
	case "float32", "float64":
		return "float64"
	case "bool":
		return "bool"
	case "time.Time":
		return "time"
	case "string":
		return "string"
	default:
		return "none"
	}
}

func mapRowsToUtils(rows []map[string]any) []utils.M {
	list := make([]utils.M, 0, len(rows))
	for _, row := range rows {
		item := utils.M{}
		for key, value := range row {
			item[key] = value
		}
		list = append(list, item)
	}
	return list
}

func headersFromRows(list []utils.M, fallback HeaderList) HeaderList {
	if len(fallback) > 0 {
		return fallback
	}
	var header HeaderList
	var headList []string
	for _, row := range list {
		for key := range row {
			if !slices.Contains(headList, key) {
				headList = append(headList, key)
				header = append(header, Header{
					Name: key,
					Type: getDataType(row[key]),
				})
			}
		}
	}
	sort.Sort(header)
	return header
}

func getRawBucketChildren(db *storm.DB, name string) ([]string, error) {
	var list []string
	err := db.Bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(name))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			if v != nil || strings.HasPrefix(string(k), "__storm") {
				return nil
			}
			list = append(list, string(k))
			return nil
		})
	})
	sort.Strings(list)
	return list, err
}

func deleteRawBucket(db *storm.DB, parts []string) error {
	if len(parts) == 0 {
		return fmt.Errorf("bucket is empty")
	}
	return db.Bolt.Update(func(tx *bbolt.Tx) error {
		if len(parts) == 1 {
			return tx.DeleteBucket([]byte(parts[0]))
		}
		var b *bbolt.Bucket
		for _, part := range parts[:len(parts)-1] {
			if b == nil {
				b = tx.Bucket([]byte(part))
			} else {
				b = b.Bucket([]byte(part))
			}
			if b == nil {
				return fmt.Errorf("bucket not found")
			}
		}
		return b.DeleteBucket([]byte(parts[len(parts)-1]))
	})
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
func getTableData(db *storm.DB, name []string, start, limit int, query []Query, reverse bool) ([]utils.M, error) {
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
		c := &Cursor{C: b.Cursor(), Reverse: reverse}
		if len(query) > 0 && query[0].Index {
			rows, err := findData(b, query[0].Field, query[0].Value, &index.Options{
				Reverse: reverse,
				Skip:    start,
				Limit:   limit,
			})
			if err != nil {
				return err
			}
			for _, k := range rows {
				v := b.Get(k)
				if ds, ok := rawRowFromKV(k, v); ok {
					list = append(list, ds)
				}
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
				ds, ok := rawRowFromKV(k, v)
				if !ok {
					continue
				}
				if len(list) >= limit {
					break
				}

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
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if !isRawDataEntry(k, v) {
				continue
			}
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

func isRawDataEntry(key, value []byte) bool {
	// bbolt 中 value == nil 表示子 bucket；__storm* 是 storm-rev 元数据或索引 bucket。
	// 普通 db.Set 列表只能把真正的 KV 记录当作数据行。
	return value != nil && !bytes.HasPrefix(key, []byte("__storm"))
}

func rawRowFromKV(key, value []byte) (utils.M, bool) {
	if !isRawDataEntry(key, value) {
		return nil, false
	}
	var row map[string]any
	if err := json.Unmarshal(value, &row); err == nil && row != nil {
		ds := utils.M{}
		for name, val := range row {
			ds[name] = val
		}
		if _, hasID := ds["id"]; !hasID {
			if _, hasKey := ds["key"]; !hasKey {
				ds["key"] = rawBoltKey(key)
			}
		}
		return ds, true
	}

	var val any
	if err := json.Unmarshal(value, &val); err != nil {
		val = string(value)
	}
	return utils.M{
		"key":   rawBoltKey(key),
		"value": val,
	}, true
}

func rawBoltKey(key []byte) any {
	if isPrintableBoltKey(key) {
		return string(key)
	}
	if len(key) == 8 {
		if id, err := numberfromb(key); err == nil {
			return id
		}
	}
	return fmt.Sprintf("%x", key)
}

func isPrintableBoltKey(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	for _, ch := range key {
		if ch < 32 || ch > 126 {
			return false
		}
	}
	return true
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
		Db     string  `json:"db"`
		Table  string  `json:"table"`  //table 名
		Page   int     `json:"page"`   //页
		Number int     `json:"number"` //记录数
		Query  []Query `json:"query"`
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return nil, err
	}

	db := b.getDB(params.Db)
	go export(db, params.Table, params.Query, params.Page, params.Number)

	return nil, nil
}

// 执行导出任务
func export(db *storm.DB, table string, query []Query, page, number int) {
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
	list, _, _, err := queryTableData(db, tableArr, page, number, query, false)
	if err != nil {
		taskItem.Status = fmt.Sprintf("打开导出数据列表出错：%v", err)
		common.BDB.Set("export", "task", exportList)
		exportLog.Error(fmt.Errorf("打开导出数据列表出错：%v", err))
		return
	}
	buf := new(bytes.Buffer)
	zipFile := zip.NewWriter(buf)
	defer zipFile.Close()
	jsonFile, err := zipFile.Create(strings.Join(tableArr, "-") + ".json")
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
		Db    string    `json:"db"`
		Table []string  `json:"table"` //表名
		Data  []utils.M `json:"data"`  //数据列表
	}

	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}

	db := b.getDB(params.Db)
	ref, err := resolveTableRef(db, params.Table)
	if err == nil && ref.Schema {
		if err := importSchemaTableData(db, ref, params.Data); err != nil {
			return fmt.Errorf("导入数据出错: %v", err)
		}
		return nil
	}
	err = db.Bolt.Update(func(tx *bbolt.Tx) error {
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
		var top float64 = 0
		for _, item := range params.Data {
			tmp := item["id"].(float64)
			if tmp > top {
				top = tmp
			}
			k, err := toBytes(item["id"])
			if err != nil {
				return fmt.Errorf("生成KEY出错: %v", err)
			}
			err = b.Put(k, item.ToJson())
			if err != nil {
				return fmt.Errorf("导入数据出错: %v", err)
			}
		}
		meta := b.Bucket([]byte("__storm_metadata"))
		if meta != nil {
			count := meta.Get([]byte("Idcounter"))
			counter, err := numberfromb(count)
			if err != nil {
				counter = 0
			}
			counter += int64(top)
			counterb, _ := numbertob(counter)
			meta.Put([]byte("Idcounter"), counterb)
		} else {
			meta, _ := b.CreateBucket([]byte("__storm_metadata"))
			counter := int64(top)
			counterb, _ := numbertob(counter)
			meta.Put([]byte("Idcounter"), counterb)
			meta.Put([]byte("codec"), []byte("json"))
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

// 备份任务类型
type BackupTask struct {
	Name         string `json:"name"`          // 任务名称
	Status       string `json:"status"`        // 状态
	FilePath     string `json:"file_path"`     // 备份文件路径
	FileName     string `json:"file_name"`     // 备份文件名
	Size         int64  `json:"size"`          // 文件大小
	CreatedDate  int64  `json:"created_date"`  // 创建时间
	CompleteDate int64  `json:"complete_date"` // 完成时间
}

type BackupTaskList []*BackupTask

// 添加备份任务
func (b *BackupTaskList) Add(item *BackupTask) {
	bs := *b
	if len(bs) > 10 {
		rmItem := bs[0]
		bs = append(bs[1:], item)
		os.Remove(rmItem.FilePath)
	} else {
		bs = append(bs, item)
	}
	*b = bs
}

var backupLog = components.NewSysLog("db_backup_")

// 备份当前数据库文件 - 异步执行
func (b *BoltdbManageController) ActionBackupCurrent(args []byte) error {
	var params struct {
		Db string `json:"db"`
	}
	err := json.Unmarshal(args, &params)
	if err != nil {
		return err
	}
	go performBackup(params.Db)
	return nil
}

// 执行备份任务
func performBackup(dbName string) {
	dbPath := common.Conf.BDB.Path
	if dbName != "" && dbName != "sys" {
		if p, ok := common.Conf.BDB.List[dbName]; ok {
			dbPath = p
		} else {
			for k, p := range common.Conf.BDB.List {
				if k == dbName || p == dbName {
					dbPath = p
					break
				}
			}
		}
	}
	pathStr, name := path.Split(dbPath)
	db := common.GetDB(dbName)
	backPath := path.Join(pathStr, "backup", time.Now().Format("20060102"))

	showName := "数据库备份"
	if dbName != "" {
		showName = fmt.Sprintf("数据库备份(%s)", dbName)
	}

	taskItem := &BackupTask{
		Name:         showName,
		Status:       "开始备份",
		CreatedDate:  time.Now().Unix(),
		CompleteDate: 0,
	}

	// 获取或创建备份任务列表
	var taskList BackupTaskList
	err := common.BDB.Get("backup", "task", &taskList)
	if err != nil && err != storm.ErrNotFound {
		backupLog.Error(fmt.Errorf("打开备份任务列表出错: %v", err))
		return
	}
	if err == storm.ErrNotFound {
		taskList = BackupTaskList{}
	}
	taskList.Add(taskItem)
	err = common.BDB.Set("backup", "task", taskList)
	if err != nil {
		backupLog.Error(fmt.Errorf("保存备份任务出错: %v", err))
		return
	}

	// 创建备份目录
	if !utils.PathExists(backPath) {
		err := os.MkdirAll(backPath, 0755)
		if err != nil {
			taskItem.Status = fmt.Sprintf("创建备份目录出错: %v", err)
			common.BDB.Set("backup", "task", taskList)
			backupLog.Error(fmt.Errorf("创建备份目录出错: %v", err))
			return
		}
	}

	zipFileName := path.Join(backPath, fmt.Sprintf("%s.zip", time.Now().Format("20060102150405")))
	f, err := os.OpenFile(zipFileName, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		taskItem.Status = fmt.Sprintf("打开ZIP文件出错: %v", err)
		common.BDB.Set("backup", "task", taskList)
		backupLog.Error(fmt.Errorf("打开ZIP文件出错: %v", err))
		return
	}
	defer f.Close()

	zipFile := zip.NewWriter(f)
	defer zipFile.Close()

	zf, err := zipFile.Create(name)
	if err != nil {
		taskItem.Status = fmt.Sprintf("创建ZIP文件出错: %v", err)
		common.BDB.Set("backup", "task", taskList)
		backupLog.Error(fmt.Errorf("创建ZIP文件出错: %v", err))
		return
	}

	tmpFile, err := os.CreateTemp(backPath, "backup-*.db")
	if err != nil {
		taskItem.Status = fmt.Sprintf("创建临时备份文件出错: %v", err)
		common.BDB.Set("backup", "task", taskList)
		backupLog.Error(fmt.Errorf("创建临时备份文件出错: %v", err))
		return
	}
	tmpFileName := tmpFile.Name()
	defer os.Remove(tmpFileName)
	if err = tmpFile.Close(); err != nil {
		taskItem.Status = fmt.Sprintf("关闭临时备份文件出错: %v", err)
		common.BDB.Set("backup", "task", taskList)
		backupLog.Error(fmt.Errorf("关闭临时备份文件出错: %v", err))
		return
	}

	// 使用 bbolt 的 CopyFile 生成一致性的数据库快照，再写入 ZIP 文件。
	err = db.Bolt.View(func(tx *bbolt.Tx) error {
		return tx.CopyFile(tmpFileName, 0600)
	})
	if err != nil {
		taskItem.Status = fmt.Sprintf("复制数据库文件出错: %v", err)
		common.BDB.Set("backup", "task", taskList)
		backupLog.Error(fmt.Errorf("复制数据库文件出错: %v", err))
		return
	}

	tmpReadFile, err := os.Open(tmpFileName)
	if err != nil {
		taskItem.Status = fmt.Sprintf("打开临时备份文件出错: %v", err)
		common.BDB.Set("backup", "task", taskList)
		backupLog.Error(fmt.Errorf("打开临时备份文件出错: %v", err))
		return
	}
	defer tmpReadFile.Close()

	_, err = io.Copy(zf, tmpReadFile)
	if err != nil {
		taskItem.Status = fmt.Sprintf("写入ZIP文件出错: %v", err)
		common.BDB.Set("backup", "task", taskList)
		backupLog.Error(fmt.Errorf("写入ZIP文件出错: %v", err))
		return
	}

	zipFile.Close()

	// 获取文件信息
	fileInfo, err := os.Stat(zipFileName)
	if err != nil {
		taskItem.Status = fmt.Sprintf("获取文件信息出错: %v", err)
		common.BDB.Set("backup", "task", taskList)
		backupLog.Error(fmt.Errorf("获取文件信息出错: %v", err))
		return
	}

	// 更新任务状态为完成
	taskItem.Status = "备份完成"
	taskItem.FilePath = zipFileName
	taskItem.FileName = filepath.Base(zipFileName)
	taskItem.Size = fileInfo.Size()
	taskItem.CompleteDate = time.Now().Unix()
	err = common.BDB.Set("backup", "task", taskList)
	if err != nil {
		backupLog.Error(fmt.Errorf("更新备份任务状态出错: %v", err))
	}
}

// 获取备份任务列表
func (b *BoltdbManageController) ActionBackupTaskList() (BackupTaskList, error) {
	var taskList BackupTaskList
	err := common.BDB.Get("backup", "task", &taskList)
	if err != nil && err != storm.ErrNotFound {
		return nil, fmt.Errorf("打开备份任务列表出错: %v", err)
	}
	if err == storm.ErrNotFound {
		return BackupTaskList{}, nil
	}
	slices.Reverse(taskList)
	return taskList, nil
}

// 下载现有备份文件
func (b *BoltdbManageController) ActionDownloadBackup() {
	filePath := b.c.Query("f")
	if filePath == "" {
		b.c.JSON(400, gin.H{"error": "缺少文件路径参数"})
		return
	}
	if !utils.PathExists(filePath) {
		b.c.JSON(404, gin.H{"error": "文件不存在"})
		return
	}
	b.c.FileAttachment(filePath, path.Base(filePath))
}

type BackupFileInfo struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	ModifyTime int64  `json:"modify_time"`
}

func (b *BoltdbManageController) ActionBackupList() ([]BackupFileInfo, error) {
	pathStr, _ := path.Split(common.Conf.BDB.Path)
	backPath := path.Join(pathStr, "backup")
	if !utils.PathExists(backPath) {
		return []BackupFileInfo{}, nil
	}
	var list []BackupFileInfo
	err := filepath.Walk(backPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".zip") {
			relPath, _ := filepath.Rel(pathStr, filePath)
			list = append(list, BackupFileInfo{
				Name:       info.Name(),
				Path:       relPath,
				Size:       info.Size(),
				ModifyTime: info.ModTime().Unix(),
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("遍历备份目录出错: %v", err)
	}
	slices.SortFunc(list, func(a, b BackupFileInfo) int {
		if b.ModifyTime > a.ModifyTime {
			return 1
		}
		if b.ModifyTime < a.ModifyTime {
			return -1
		}
		return 0
	})
	return list, nil
}

// 获取 BoltDB 状态统计信息
func (b *BoltdbManageController) ActionStats(args []byte) (utils.M, error) {
	var params struct {
		Db string `json:"db"`
	}
	_ = json.Unmarshal(args, &params)
	db := b.getDB(params.Db)
	stats := db.Bolt.Stats()
	return utils.M{
		"free_page_n":     stats.FreePageN,
		"pending_page_n":  stats.PendingPageN,
		"free_alloc":      stats.FreeAlloc,
		"freelist_in_use": stats.FreelistInuse,
		"tx_n":            stats.TxN,
		"open_tx_n":       stats.OpenTxN,
		"tx_stats": utils.M{
			"page_count":     stats.TxStats.PageCount,
			"page_alloc":     stats.TxStats.PageAlloc,
			"cursor_count":   stats.TxStats.CursorCount,
			"node_count":     stats.TxStats.NodeCount,
			"node_deref":     stats.TxStats.NodeDeref,
			"rebalance":      stats.TxStats.Rebalance,
			"rebalance_time": stats.TxStats.RebalanceTime,
			"split":          stats.TxStats.Split,
			"spill":          stats.TxStats.Spill,
			"spill_time":     stats.TxStats.SpillTime,
			"write":          stats.TxStats.Write,
			"write_time":     stats.TxStats.WriteTime,
		},
	}, nil
}

type localStoredSchemaField struct {
	Name           string         `json:"name"`
	JSON           string         `json:"json"`
	Type           string         `json:"type"`
	Index          string         `json:"index,omitempty"`
	ID             bool           `json:"id,omitempty"`
	Increment      bool           `json:"increment,omitempty"`
	IncrementStart int64          `json:"increment_start,omitempty"`
	Integer        bool           `json:"integer,omitempty"`
	Composites     map[string]int `json:"composites,omitempty"`
}

type localStoredSchema struct {
	Table  string                   `json:"table"`
	Fields []localStoredSchemaField `json:"fields"`
	ID     string                   `json:"id"`
}

// 获取当前表的 metadata 元数据
func (b *BoltdbManageController) ActionGetMetadata(args []byte) (utils.M, error) {
	var params struct {
		Db    string `json:"db"`
		Table string `json:"table"` // table 名
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return nil, err
	}

	db := b.getDB(params.Db)
	tables := strings.Split(params.Table, "|")
	ref, err := resolveTableRef(db, tables)
	if err != nil {
		return nil, err
	}

	var from, table string
	if len(ref.Parts) >= 2 {
		from = ref.Parts[0]
		table = ref.Parts[1]
	} else if len(ref.Parts) == 1 {
		table = ref.Parts[0]
	} else {
		return nil, fmt.Errorf("表名解析错误")
	}

	metadata := utils.M{}

	// 1. 优先使用 ListColumns 获取已存的列元数据
	columns, err := db.ListColumns(from, table)
	if err == nil {
		fields := make([]localStoredSchemaField, len(columns))
		var idField string
		for i, c := range columns {
			fields[i] = localStoredSchemaField{
				Name:           c.Name,
				JSON:           c.JSON,
				Type:           c.Type,
				Index:          c.Index,
				ID:             c.ID,
				Increment:      c.Increment,
				IncrementStart: c.IncrementStart,
				Integer:        c.Integer,
				Composites:     c.Composites,
			}
			if c.ID {
				idField = c.Name
			}
		}
		metadata["schema"] = localStoredSchema{
			Table:  table,
			Fields: fields,
			ID:     idField,
		}
	}

	// 2. 遍历其它字段（如 codec, Idcounter）
	_ = db.Bolt.View(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		for _, v := range ref.Parts {
			if b != nil {
				b = b.Bucket([]byte(v))
			} else {
				b = tx.Bucket([]byte(v))
			}
		}
		if b == nil {
			return nil
		}

		metaBucket := b.Bucket([]byte("__storm_metadata"))
		if metaBucket == nil {
			return nil
		}

		return metaBucket.ForEach(func(k, v []byte) error {
			keyStr := string(k)
			if keyStr == "schema" {
				if _, ok := metadata["schema"]; !ok {
					var schema map[string]any
					if err := json.Unmarshal(v, &schema); err == nil {
						metadata[keyStr] = schema
					} else {
						metadata[keyStr] = string(v)
					}
				}
			} else if strings.HasSuffix(keyStr, "counter") {
				if counterVal, err := numberfromb(v); err == nil {
					metadata[keyStr] = counterVal
				} else {
					metadata[keyStr] = string(v)
				}
			} else {
				metadata[keyStr] = string(v)
			}
			return nil
		})
	})

	return metadata, nil
}

// 保存当前表的 metadata 元数据
func (b *BoltdbManageController) ActionSaveMetadata(args []byte) error {
	var params struct {
		Db       string         `json:"db"`
		Table    string         `json:"table"`
		Metadata map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return err
	}

	db := b.getDB(params.Db)
	tables := strings.Split(params.Table, "|")
	ref, err := resolveTableRef(db, tables)
	if err != nil {
		return err
	}

	var from, table string
	if len(ref.Parts) >= 2 {
		from = ref.Parts[0]
		table = ref.Parts[1]
	} else if len(ref.Parts) == 1 {
		table = ref.Parts[0]
	} else {
		return fmt.Errorf("表名解析错误")
	}

	// 1. 确保表 Bucket 在数据库中存在，否则创建它
	err = db.Bolt.Update(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		var err error
		for _, v := range ref.Parts {
			if b != nil {
				b, err = b.CreateBucketIfNotExists([]byte(v))
			} else {
				b, err = tx.CreateBucketIfNotExists([]byte(v))
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("创建表 Bucket 失败: %v", err)
	}

	// 2. 使用 SetTableMetadata 保存 schema 字段（这会自动更新持久化元数据并刷新外部索引）
	if schemaVal, ok := params.Metadata["schema"]; ok {
		rawSchema, err := json.Marshal(schemaVal)
		if err != nil {
			return err
		}
		var schema localStoredSchema
		if err := json.Unmarshal(rawSchema, &schema); err != nil {
			return err
		}

		columns := make([]storm.ColumnInfo, len(schema.Fields))
		for i, f := range schema.Fields {
			columns[i] = storm.ColumnInfo{
				Name:           f.Name,
				JSON:           f.JSON,
				Type:           f.Type,
				Index:          f.Index,
				ID:             f.ID,
				Increment:      f.Increment,
				IncrementStart: f.IncrementStart,
				Integer:        f.Integer,
				Composites:     f.Composites,
			}
		}

		if err := db.SetTableMetadata(from, table, columns); err != nil {
			return fmt.Errorf("设置表元数据失败: %v", err)
		}
	}

	// 3. 写入其它的元数据值（比如 codec, Idcounter 等）
	return db.Bolt.Update(func(tx *bbolt.Tx) error {
		var b *bbolt.Bucket
		for _, v := range ref.Parts {
			if b != nil {
				b = b.Bucket([]byte(v))
			} else {
				b = tx.Bucket([]byte(v))
			}
		}
		if b == nil {
			return fmt.Errorf("bucket not found")
		}

		metaBucket, err := b.CreateBucketIfNotExists([]byte("__storm_metadata"))
		if err != nil {
			return fmt.Errorf("创建 metadata bucket 失败: %v", err)
		}

		for k, v := range params.Metadata {
			if k == "schema" {
				continue // 已经保存过了
			}

			var valBytes []byte
			var err error
			if strings.HasSuffix(k, "counter") {
				var countInt int64
				switch cv := v.(type) {
				case float64:
					countInt = int64(cv)
				case int64:
					countInt = cv
				case string:
					fmt.Sscanf(cv, "%d", &countInt)
				default:
					countInt = 0
				}
				valBytes, err = numbertob(countInt)
				if err != nil {
					return fmt.Errorf("序列化 counter 失败: %v", err)
				}
			} else {
				valBytes = []byte(fmt.Sprintf("%v", v))
			}

			if err := metaBucket.Put([]byte(k), valBytes); err != nil {
				return fmt.Errorf("保存 %s 失败: %v", k, err)
			}
		}
		return nil
	})
}

// 解析 Go 结构体代码并自动生成 metadata schema
func (b *BoltdbManageController) ActionParseGolangStruct(args []byte) (*localStoredSchema, error) {
	var params struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return nil, err
	}

	schema, err := parseStructCode(params.Code)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func parseStructCode(code string) (*localStoredSchema, error) {
	code = strings.TrimSpace(code)
	if code == "" {
		return nil, fmt.Errorf("代码为空")
	}

	var src string
	if !strings.HasPrefix(code, "package ") {
		src = "package dummy\n\n" + code
	} else {
		src = code
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "", src, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("解析 Go 代码失败: %v", err)
	}

	var foundSchema *localStoredSchema
	var parseErr error

	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			return true
		}
		typeSpec, ok := n.(*ast.TypeSpec)
		if !ok {
			return true
		}

		structType, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			return true
		}

		tableName := typeSpec.Name.Name
		schema := &localStoredSchema{
			Table: tableName,
		}

		if structType.Fields == nil {
			foundSchema = schema
			return false
		}

		for _, field := range structType.Fields.List {
			var fieldNames []string
			if len(field.Names) == 0 {
				typeName := getTypeNameString(field.Type)
				parts := strings.Split(typeName, ".")
				fieldNames = []string{parts[len(parts)-1]}
			} else {
				for _, name := range field.Names {
					fieldNames = append(fieldNames, name.Name)
				}
			}

			fieldTypeStr := getTypeNameString(field.Type)

			var tagVal string
			if field.Tag != nil {
				tagVal = strings.Trim(field.Tag.Value, "`")
			}
			structTag := reflect.StructTag(tagVal)

			jsonTag := structTag.Get("json")
			if jsonTag == "-" {
				continue
			}
			jsonName := strings.Split(jsonTag, ",")[0]

			stormTag := structTag.Get("storm")

			for _, fieldName := range fieldNames {
				finalJsonName := jsonName
				if finalJsonName == "" {
					finalJsonName = fieldName
				}

				f := localStoredSchemaField{
					Name: fieldName,
					JSON: finalJsonName,
					Type: fieldTypeStr,
				}

				if stormTag != "" {
					parts := strings.Split(stormTag, ",")
					for _, part := range parts {
						part = strings.TrimSpace(part)
						switch part {
						case "id":
							f.ID = true
							schema.ID = fieldName
						case "increment":
							f.Increment = true
						case "index":
							f.Index = "index"
						case "unique":
							f.Index = "unique"
						}
					}
				}

				f.Integer = isIntegerType(fieldTypeStr)
				schema.Fields = append(schema.Fields, f)
			}
		}

		foundSchema = schema
		return false
	})

	if parseErr != nil {
		return nil, parseErr
	}
	if foundSchema == nil {
		return nil, fmt.Errorf("未在代码中找到 struct 定义")
	}

	return foundSchema, nil
}

func getTypeNameString(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return getTypeNameString(t.X) + "." + t.Sel.Name
	case *ast.StarExpr:
		return "*" + getTypeNameString(t.X)
	case *ast.ArrayType:
		return "[]" + getTypeNameString(t.Elt)
	case *ast.MapType:
		return "map[" + getTypeNameString(t.Key) + "]" + getTypeNameString(t.Value)
	case *ast.InterfaceType:
		return "interface{}"
	default:
		return "string"
	}
}

func isIntegerType(t string) bool {
	switch t {
	case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		return true
	default:
		return false
	}
}

// 接收 schema JSON，反向生成 Go Struct 代码
func (b *BoltdbManageController) ActionGenerateGolangStruct(args []byte) (utils.M, error) {
	var params struct {
		Schema *localStoredSchema `json:"schema"`
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return nil, err
	}

	code, err := generateGoStruct(params.Schema)
	if err != nil {
		return nil, err
	}

	return utils.M{"code": code}, nil
}

func generateGoStruct(schema *localStoredSchema) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("schema 不能为空")
	}

	tableName := schema.Table
	if tableName == "" {
		tableName = "Record"
	}

	var sb strings.Builder
	sb.WriteString("package dummy\n\n") // 用 dummy package 包装以便 go/format
	sb.WriteString(fmt.Sprintf("type %s struct {\n", tableName))

	for _, f := range schema.Fields {
		if f.Name == "" {
			continue
		}

		var stormTags []string
		if f.ID {
			stormTags = append(stormTags, "id")
		}
		if f.Increment {
			stormTags = append(stormTags, "increment")
		}
		if f.Index != "" {
			stormTags = append(stormTags, f.Index)
		}

		var tags []string
		if len(stormTags) > 0 {
			tags = append(tags, fmt.Sprintf(`storm:"%s"`, strings.Join(stormTags, ",")))
		}
		
		jsonName := f.JSON
		if jsonName == "" {
			jsonName = f.Name
		}
		tags = append(tags, fmt.Sprintf(`json:"%s"`, jsonName))

		tagStr := ""
		if len(tags) > 0 {
			tagStr = fmt.Sprintf(" `%s`", strings.Join(tags, " "))
		}

		fieldType := f.Type
		if fieldType == "" {
			if f.Integer {
				fieldType = "int64"
			} else {
				fieldType = "string"
			}
		}

		sb.WriteString(fmt.Sprintf("\t%s %s%s\n", f.Name, fieldType, tagStr))
	}

	sb.WriteString("}\n")

	formatted, err := format.Source([]byte(sb.String()))
	if err != nil {
		raw := sb.String()
		return strings.TrimPrefix(raw, "package dummy\n\n"), nil
	}

	res := string(formatted)
	res = strings.TrimPrefix(res, "package dummy\n\n")
	res = strings.TrimSpace(res)
	return res, nil
}

// 刷新表索引
func (b *BoltdbManageController) ActionReIndex(args []byte) error {
	var params struct {
		Db    string `json:"db"`
		Table string `json:"table"` // table 名
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return err
	}

	db := b.getDB(params.Db)
	tables := strings.Split(params.Table, "|")
	ref, err := resolveTableRef(db, tables)
	if err != nil {
		return err
	}

	var from, table string
	if len(ref.Parts) >= 2 {
		from = ref.Parts[0]
		table = ref.Parts[1]
	} else if len(ref.Parts) == 1 {
		table = ref.Parts[0]
	} else {
		return fmt.Errorf("表名解析错误")
	}

	if !ref.Schema {
		return fmt.Errorf("当前表没有 storm-rev schema 元数据，无法重建索引")
	}

	var node storm.Node
	if from != "" {
		node = db.From(from).From(table)
	} else {
		node = db.From(table)
	}

	return node.ReIndex(nil)
}



