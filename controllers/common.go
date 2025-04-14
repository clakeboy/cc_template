package controllers

import (
	"cc_template/components"
	"cc_template/models"
	"fmt"
	"net/http"
	"time"

	"github.com/clakeboy/storm-rev/q"
	"github.com/gin-gonic/gin"
)

type Condition struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

func explainQueryCondition(conditions []*Condition) []q.Matcher {
	var matcher []q.Matcher
	for _, v := range conditions {
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
