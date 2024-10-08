package controllers

import (
	"cc_template/common"
	"cc_template/models"
	"fmt"
	"os"
	"testing"

	"github.com/asdine/storm/v3"
	"github.com/asdine/storm/v3/q"
	"github.com/clakeboy/golib/utils"
)

func init() {
	var err error
	common.BDB, err = storm.Open("../db/btm-20240919.db")
	if err != nil {
		fmt.Println("open storm database error:", err)
		os.Exit(-1)
	}
}

func TestDb(t *testing.T) {
	model := models.CommonModel[RequestData]{
		Order: "DESC",
		Node:  common.BDB.From("request"),
	}
	run := utils.NewExecTime()
	run.Start()
	count, err := model.GetSum("Id", q.Eq("Id", "256"))
	if err != nil {
		t.Error(err)
	}
	run.End(true)
	fmt.Println(count)
	run.Start()
	count, err = model.GetSumRaw("id", q.Eq("Id", 256))
	if err != nil {
		t.Error(err)
	}
	run.End(true)
	fmt.Println(count)
}

// func TestShowTables(t *testing.T) {
// 	list, err := getBoltdbList(common.BDB)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	utils.PrintAny(list)

// 	list, err = getChildData(common.BDB, "request")
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	utils.PrintAny(list)

// 	count, err := getTableCount(common.BDB, []string{"request"})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	fmt.Println(count)
// }

type RequestData struct {
	Id          int    `storm:"id,increment" json:"id"` //主键,自增长
	SerialNo    string `storm:"index" json:"serial_no"` //生成的报文ID
	BusData     string `json:"bus_data"`                //业务内容
	ReqData     string `json:"req_data"`                //报文内容
	RespData    string `json:"response_data"`           //报文回复内容
	BusRespData string `json:"bus_response_data"`       // 业务返回内容
	CreatedDate int64  `json:"created_date"`            //报文生成时间
	RespDate    int64  `json:"response_date"`           //报文回复时间
}

// func Prof() {
// 	model := models.CommonModel[RequestData]{
// 		Order: "DESC",
// 		Node:  common.BDB.From("request"),
// 	}
// 	run := utils.NewExecTime()
// 	run.Start()
// 	count, err := model.GetSum("Id")
// 	if err != nil {
// 		panic(err)
// 	}
// 	run.End(true)
// 	fmt.Println(count)
// 	run.Start()
// 	count, err = model.GetSumRaw("id")
// 	if err != nil {
// 		panic(err)
// 	}
// 	run.End(true)
// 	fmt.Println(count)
// 	println("pprof done")
// }
