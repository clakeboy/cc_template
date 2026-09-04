package controllers

import (
	"cc_template/components"
	"cc_template/models"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"time"

	"github.com/clakeboy/storm-rev/v2/q"
	"github.com/gin-gonic/gin"
)

type Condition struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// fieldKindOf 取模型结构体中该字段的基础类型（已剥离指针）。
// 找不到字段或 sample 为 nil 时返回 reflect.Invalid。
func fieldKindOf(sample any, name string) reflect.Kind {
	if sample == nil || name == "" {
		return reflect.Invalid
	}
	t := reflect.TypeOf(sample)
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return reflect.Invalid
	}
	field, ok := t.FieldByName(name)
	if !ok {
		return reflect.Invalid
	}
	ft := field.Type
	for ft.Kind() == reflect.Ptr {
		ft = ft.Elem()
	}
	return ft.Kind()
}

// isIntKind 判断字段是否为整数类型
func isIntKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

// normalizeConditionValue 按目标字段类型归一 JSON 弱类型数值。
// JSON 数字反序列化后统一是 float64，而 storm 生成索引 token 时整数走 8 字节 Big-Endian、
// float64 走 codec.Marshal，token 不一致会导致带 index 的字段命中 0 条且不回退全表。
// 仅在字段声明为整数类型时才转换；字段为浮点类型时保持 float64，
// 且值真有小数时绝不转换，避免破坏 float 字段的查询。
func normalizeConditionValue(v any, kind reflect.Kind) any {
	if !isIntKind(kind) {
		return v
	}
	switch val := v.(type) {
	case float64:
		if val == math.Trunc(val) && !math.IsInf(val, 0) {
			return int64(val)
		}
	case []any:
		for i, item := range val {
			val[i] = normalizeConditionValue(item, kind)
		}
		return val
	}
	return v
}

// ExplainQueryCondition 解析查询条件。sample 传对应模型零值（如 models.ContentData{}），
// 用于按字段声明类型归一条件值，防止 JSON 弱类型导致索引查询失配。
func ExplainQueryCondition(conditions []*Condition, sample any) []q.Matcher {
	var matcher []q.Matcher
	for _, v := range conditions {
		v.Value = normalizeConditionValue(v.Value, fieldKindOf(sample, v.Name))
		var match q.Matcher
		switch v.Type {
		case "eq":
			match = q.Eq(v.Name, v.Value)
		case "re":
			match = q.Re(v.Name, fmt.Sprintf("^%s", v.Value))
		case "lt":
			match = q.Lt(v.Name, v.Value)
		case "lte":
			match = q.Lte(v.Name, v.Value)
		case "gt":
			match = q.Gt(v.Name, v.Value)
		case "gte":
			match = q.Gte(v.Name, v.Value)
		case "in":
			match = q.In(v.Name, v.Value)
		case "ne":
			//不等于
			match = q.Not(q.Eq(v.Name, v.Value))
		}
		matcher = append(matcher, match)
	}

	return matcher
}

func AuthAccountLogin(c *gin.Context) *models.AccountData {
	user, err := components.AuthAccount(c)
	if err != nil {
		c.Redirect(http.StatusFound, "/front/def/login")
		return nil
	}

	return user
}

func FormatDate(dt int64) string {
	if dt == 0 {
		return "时间错误"
	}
	return time.Unix(dt, 0).Format("2006-01-02 15:04")
}

func YN(cond bool, yes string, no string) string {
	if cond {
		return yes
	}
	return no
}
