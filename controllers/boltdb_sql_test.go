package controllers

import (
	"encoding/json"
	"testing"

	"cc_template/common"
	"cc_template/models"

	"github.com/clakeboy/golib/utils"
	"github.com/clakeboy/storm-rev"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

type boltSQLRecord struct {
	ID     int    `storm:"id,increment" json:"id"`
	Name   string `storm:"index" json:"name"`
	Status string `storm:"index" json:"status"`
	Amount int    `json:"amount"`
}

func withBoltSQLTestDB(t *testing.T) (*storm.DB, *BoltdbManageController) {
	t.Helper()
	oldDB := common.BDB
	db, err := storm.Open(t.TempDir() + "/bolt_sql.db")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		common.BDB = oldDB
	})
	common.BDB = db
	controller := NewBoltdbManageController(nil)
	return db, controller
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return raw
}

func seedBoltSQLRecords(t *testing.T, db *storm.DB) {
	t.Helper()
	require.NoError(t, db.From("repo").SaveAll([]*boltSQLRecord{
		{Name: "Alpha", Status: "open", Amount: 10},
		{Name: "Beta", Status: "closed", Amount: 20},
		{Name: "Gamma", Status: "open", Amount: 30},
	}))
}

func TestBoltdbSchemaQuerySaveAndDelete(t *testing.T) {
	db, controller := withBoltSQLTestDB(t)
	seedBoltSQLRecords(t, db)

	res, err := controller.ActionQuery(mustJSON(t, utils.M{
		"table":  "repo|boltSQLRecord",
		"page":   1,
		"number": 10,
		"query": []Query{{
			Field: "status",
			Type:  "eq",
			Value: "open",
		}},
	}))
	require.NoError(t, err)
	require.Equal(t, 2, res["count"])
	require.Len(t, res["list"], 2)

	require.NoError(t, controller.ActionSave(mustJSON(t, utils.M{
		"table": "repo|boltSQLRecord",
		"id":    1,
		"data":  `{"id":1,"name":"Alpha2","status":"open","amount":11}`,
	})))
	var updated []boltSQLRecord
	require.NoError(t, db.From("repo").Find("Name", "Alpha2", &updated))
	require.Len(t, updated, 1)
	require.Equal(t, 11, updated[0].Amount)
	var old []boltSQLRecord
	err = db.From("repo").Find("Name", "Alpha", &old)
	require.ErrorIs(t, err, storm.ErrNotFound)

	require.NoError(t, controller.ActionDelete(mustJSON(t, utils.M{
		"table":   "repo|boltSQLRecord",
		"id_list": []int{2},
	})))
	manager, err := db.From("repo").SQL()
	require.NoError(t, err)
	count, err := manager.Count("SELECT COUNT(*) FROM boltSQLRecord WHERE id = ?", 2)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestBoltdbSQLExec(t *testing.T) {
	db, controller := withBoltSQLTestDB(t)
	seedBoltSQLRecords(t, db)

	countRes, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from": "repo",
		"sql":  "SELECT COUNT(*) FROM boltSQLRecord WHERE status = ?",
		"args": []any{"open"},
	}))
	require.NoError(t, err)
	require.Equal(t, "count", countRes.Kind)
	require.Equal(t, 2, countRes.Count)

	selectRes, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from": "repo",
		"sql":  "SELECT name, amount FROM boltSQLRecord WHERE status = ? ORDER BY amount ASC",
		"args": []any{"open"},
	}))
	require.NoError(t, err)
	require.Equal(t, "select", selectRes.Kind)
	require.Len(t, selectRes.List, 2)

	insertRes, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from": "repo",
		"sql":  "INSERT INTO boltSQLRecord (name, status, amount) VALUES (?, ?, ?)",
		"args": []any{"Delta", "open", 40},
	}))
	require.NoError(t, err)
	require.Equal(t, "exec", insertRes.Kind)
	require.Equal(t, 1, insertRes.RowsAffected)

	updateRes, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from": "repo",
		"sql":  "UPDATE boltSQLRecord SET status = ? WHERE name = ?",
		"args": []any{"done", "Delta"},
	}))
	require.NoError(t, err)
	require.Equal(t, 1, updateRes.RowsAffected)

	deleteRes, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from": "repo",
		"sql":  "DELETE FROM boltSQLRecord WHERE name = ?",
		"args": []any{"Delta"},
	}))
	require.NoError(t, err)
	require.Equal(t, 1, deleteRes.RowsAffected)

	_, err = controller.ActionSqlExec(mustJSON(t, utils.M{
		"from": "repo",
		"sql":  "UPDATE boltSQLRecord SET status = ?",
		"args": []any{"blocked"},
	}))
	require.ErrorIs(t, err, storm.ErrSQLUnsafeWrite)
}

func TestBoltdbSQLExecUsesSchemaMetadataWithoutRegistration(t *testing.T) {
	db, controller := withBoltSQLTestDB(t)
	require.NoError(t, db.From("group").SaveAll([]*models.GroupData{
		{Name: "admin"},
	}))

	res, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from": "group",
		"sql":  "SELECT * FROM GroupData LIMIT 10",
	}))
	require.NoError(t, err)
	require.Equal(t, "select", res.Kind)
	require.Len(t, res.List, 1)
	require.Equal(t, "admin", res.List[0]["name"])
}

func TestBoltdbLegacyBucketFallback(t *testing.T) {
	db, controller := withBoltSQLTestDB(t)
	require.NoError(t, db.Set("legacy", 1, utils.M{"id": 1, "name": "old"}))
	require.NoError(t, db.Set("legacyObject", "name-key", utils.M{"name": "object"}))
	require.NoError(t, db.Set("legacyValue", "token", "plain"))

	tree, err := controller.ActionDatabases(mustJSON(t, utils.M{}))
	require.NoError(t, err)
	var foundLegacy bool
	for _, node := range tree {
		if node.Key == "legacy" && !node.SQL {
			foundLegacy = true
			break
		}
	}
	require.True(t, foundLegacy)

	res, err := controller.ActionQuery(mustJSON(t, utils.M{
		"table":  "legacy",
		"page":   1,
		"number": 10,
	}))
	require.NoError(t, err)
	require.Equal(t, 1, res["count"])
	require.Len(t, res["list"], 1)

	objectRes, err := controller.ActionQuery(mustJSON(t, utils.M{
		"table":  "legacyObject",
		"page":   1,
		"number": 10,
	}))
	require.NoError(t, err)
	require.Equal(t, 1, objectRes["count"])
	require.Len(t, objectRes["list"], 1)
	objectRows := objectRes["list"].([]utils.M)
	require.Equal(t, "object", objectRows[0]["name"])
	require.Equal(t, "name-key", objectRows[0]["key"])

	valueRes, err := controller.ActionQuery(mustJSON(t, utils.M{
		"table":  "legacyValue",
		"page":   1,
		"number": 10,
	}))
	require.NoError(t, err)
	require.Equal(t, 1, valueRes["count"])
	require.Len(t, valueRes["list"], 1)
	valueRows := valueRes["list"].([]utils.M)
	require.Equal(t, "token", valueRows[0]["key"])
	require.Equal(t, "plain", valueRows[0]["value"])
}

func TestBoltdbMetadataEndpointsAndParsing(t *testing.T) {
	db, controller := withBoltSQLTestDB(t)

	// 1. 测试 Go Struct 解析接口
	parseRes, err := controller.ActionParseGolangStruct(mustJSON(t, utils.M{
		"code": "type TestUser struct {\n" +
			"ID        int64  `storm:\"id,increment\" json:\"id\"`\n" +
			"Username  string `storm:\"index\" json:\"username\"`\n" +
			"IsActive  bool   `json:\"is_active\"`\n" +
			"Score     float64\n" +
			"}",
	}))
	require.NoError(t, err)
	require.Equal(t, "TestUser", parseRes.Table)
	require.Equal(t, "ID", parseRes.ID)
	require.Len(t, parseRes.Fields, 4)

	// 验证各字段的标签提取和属性
	var idField, userField localStoredSchemaField
	for _, f := range parseRes.Fields {
		if f.Name == "ID" {
			idField = f
		} else if f.Name == "Username" {
			userField = f
		}
	}
	require.True(t, idField.ID)
	require.True(t, idField.Increment)
	require.True(t, idField.Integer)
	require.Equal(t, "id", idField.JSON)

	require.Equal(t, "index", userField.Index)
	require.Equal(t, "username", userField.JSON)
	require.False(t, userField.Integer)

	// 2. 准备数据，创建包含 __storm_metadata 的表
	require.NoError(t, db.Set("metadata_table", "key1", utils.M{"name": "hello"}))
	// 往 metadata_table 的 __storm_metadata 写入数据
	err = db.Bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("metadata_table"))
		meta, err := b.CreateBucketIfNotExists([]byte("__storm_metadata"))
		if err != nil {
			return err
		}
		require.NoError(t, meta.Put([]byte("codec"), []byte("json")))
		counterb, _ := numbertob(int64(123))
		require.NoError(t, meta.Put([]byte("Idcounter"), counterb))
		return nil
	})
	require.NoError(t, err)

	// 3. 测试 ActionGetMetadata 接口
	getRes, err := controller.ActionGetMetadata(mustJSON(t, utils.M{
		"table": "metadata_table",
	}))
	require.NoError(t, err)
	require.Equal(t, "json", getRes["codec"])
	require.Equal(t, int64(123), getRes["Idcounter"])

	// 4. 测试 ActionSaveMetadata 接口
	schemaRaw, _ := json.Marshal(parseRes)
	var schemaMap map[string]any
	json.Unmarshal(schemaRaw, &schemaMap)

	err = controller.ActionSaveMetadata(mustJSON(t, utils.M{
		"table": "metadata_table",
		"metadata": utils.M{
			"codec":     "json",
			"Idcounter": 456,
			"schema":    schemaMap,
		},
	}))
	require.NoError(t, err)

	// 5. 再次读取元数据以验证是否保存成功
	getRes2, err := controller.ActionGetMetadata(mustJSON(t, utils.M{
		"table": "metadata_table",
	}))
	require.NoError(t, err)
	require.Equal(t, "json", getRes2["codec"])
	require.Equal(t, int64(456), getRes2["Idcounter"])
	
	schemaRaw2, err := json.Marshal(getRes2["schema"])
	require.NoError(t, err)
	var schemaVal map[string]any
	json.Unmarshal(schemaRaw2, &schemaVal)
	require.Equal(t, "metadata_table", schemaVal["table"])

	// 6. 测试 ActionGenerateGolangStruct
	genRes, err := controller.ActionGenerateGolangStruct(mustJSON(t, utils.M{
		"schema": schemaVal,
	}))
	require.NoError(t, err)
	codeStr := genRes["code"].(string)
	require.Contains(t, codeStr, "type metadata_table struct")
	require.Contains(t, codeStr, "ID")
	require.Contains(t, codeStr, "`storm:\"id,increment,unique\" json:\"id\"`")
	require.Contains(t, codeStr, "Username")
	require.Contains(t, codeStr, "`storm:\"index\" json:\"username\"`")

	// 7. 测试 ActionReIndex
	err = controller.ActionReIndex(mustJSON(t, utils.M{
		"table": "metadata_table",
	}))
	require.NoError(t, err)
}

func TestBoltdbSQLDefaultLimit(t *testing.T) {
	db, controller := withBoltSQLTestDB(t)

	// 插入 105 条数据
	var records []*boltSQLRecord
	for i := 1; i <= 105; i++ {
		records = append(records, &boltSQLRecord{
			Name:   "Alpha",
			Status: "open",
			Amount: i,
		})
	}
	require.NoError(t, db.From("repo").SaveAll(records))

	// 1. 不带 LIMIT 的 SQL，默认限制为 100 条
	res1, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from": "repo",
		"sql":  "SELECT name, amount FROM boltSQLRecord WHERE status = ? ORDER BY amount ASC",
		"args": []any{"open"},
	}))
	require.NoError(t, err)
	require.Equal(t, "select", res1.Kind)
	require.Len(t, res1.List, 100)

	// 2. 带 LIMIT 的 SQL（例如 LIMIT 110），应该返回全部 105 条数据
	res2, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from": "repo",
		"sql":  "SELECT name, amount FROM boltSQLRecord WHERE status = ? ORDER BY amount ASC LIMIT 110",
		"args": []any{"open"},
	}))
	require.NoError(t, err)
	require.Equal(t, "select", res2.Kind)
	require.Len(t, res2.List, 105)

	// 3. 验证带有分号的 SQL 也可以正确被加上 LIMIT
	res3, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from": "repo",
		"sql":  "SELECT name, amount FROM boltSQLRecord WHERE status = ? ORDER BY amount ASC;",
		"args": []any{"open"},
	}))
	require.NoError(t, err)
	require.Equal(t, "select", res3.Kind)
	require.Len(t, res3.List, 100)

	// 4. 验证 sqlCountSelectRe 正则表达式，确保 count(...) 类型的查询语句能被正确识别以排除自动限制
	require.True(t, sqlCountSelectRe.MatchString("SELECT count(*) FROM boltSQLRecord"))
	require.True(t, sqlCountSelectRe.MatchString("SELECT COUNT(1) FROM boltSQLRecord"))
	require.True(t, sqlCountSelectRe.MatchString("SELECT count(id) FROM boltSQLRecord"))
	require.True(t, sqlCountSelectRe.MatchString("SELECT COUNT(  name ) FROM boltSQLRecord"))
	require.False(t, sqlCountSelectRe.MatchString("SELECT name, count_num FROM boltSQLRecord"))

	// 5. 验证传入 page 和 number 参数时，自动加上 LIMIT 和 OFFSET 并能够获取第二页数据
	res5, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from":   "repo",
		"sql":    "SELECT name, amount FROM boltSQLRecord WHERE status = ? ORDER BY amount ASC",
		"args":   []any{"open"},
		"page":   2,
		"number": 10,
	}))
	require.NoError(t, err)
	require.Equal(t, "select", res5.Kind)
	require.Len(t, res5.List, 10)
	// 第 2 页，首条数据的 amount 应当是 11（在 JSON 泛型解析下数值通常是 float64）
	firstRow := res5.List[0]
	// 由于 amount 是 int，我们直接用 reflect 或 map 形式获取 float64 比较
	amountVal, ok := firstRow["amount"].(float64)
	if !ok {
		// 也有可能由于 mapRowsToUtils 的具体还原，它仍然被解析为 float64 或 int/int64
		// 我们可以转为 float64
		switch v := firstRow["amount"].(type) {
		case int:
			amountVal = float64(v)
		case int64:
			amountVal = float64(v)
		}
	}
	require.Equal(t, float64(11), amountVal)
	// 验证返回的 Count 是满足 where 条件的真实总数 105，而不是单页大小 10
	require.Equal(t, 105, res5.Count)

	// 6. 验证主动传入 LIMIT 的情况，不需要计算全部 count，Count 字段直接返回当前数据列表大小（例如 LIMIT 10）
	res6, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from": "repo",
		"sql":  "SELECT name, amount FROM boltSQLRecord WHERE status = ? ORDER BY amount ASC LIMIT 10",
		"args": []any{"open"},
	}))
	require.NoError(t, err)
	require.Equal(t, "select", res6.Kind)
	require.Len(t, res6.List, 10)
	require.Equal(t, 10, res6.Count)

	// 7. 验证最后一页（返回记录数少于传入的 number），直接通过 offset + len(list) 计算出总量为 105，而不用全量 count
	res7, err := controller.ActionSqlExec(mustJSON(t, utils.M{
		"from":   "repo",
		"sql":    "SELECT name, amount FROM boltSQLRecord WHERE status = ? ORDER BY amount ASC",
		"args":   []any{"open"},
		"page":   11,
		"number": 10,
	}))
	require.NoError(t, err)
	require.Equal(t, "select", res7.Kind)
	require.Len(t, res7.List, 5)
	require.Equal(t, 105, res7.Count)
}

