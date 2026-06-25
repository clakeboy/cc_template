package controllers

import (
	"cc_template/command"
	"cc_template/common"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/clakeboy/golib/components/snowflake"
	"github.com/clakeboy/golib/utils"
	"github.com/clakeboy/storm-rev"
	"go.etcd.io/bbolt"
)

func init() {
	var err error
	//获取YAML
	common.Conf = common.NewYamlConfig(command.CmdConfFile)

	//初始化全局snowid生成器
	common.SnowFlake, err = snowflake.NewShowFlake(1, 1, 1)
	if err != nil {
		fmt.Println("初始化 SnowId 错误：", err)
		return
	}
}

func initTestDB() {
	if common.BDB == nil {
		var err error
		common.BDB, err = storm.Open("../db/sys.db")
		if err != nil {
			panic(fmt.Sprintf("open storm database error: %v", err))
		}
	}
}

func TestBolt(t *testing.T) {
	initTestDB()
	export(common.BDB, "menu|MenuData", nil, 1, 100)
	var exportList Export
	err := common.BDB.Get("export", "task", &exportList)
	if err != nil {
		return
	}
	fmt.Println("task list length:", len(exportList))
	utils.PrintAny(exportList)
}

func TestData(t *testing.T) {
	initTestDB()
	// list, err := getChildData(common.BDB, "menu")
	name := []string{"account", "AccountData", "__storm_index_Id"}
	err := common.BDB.Bolt.View(func(tx *bbolt.Tx) error {
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
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// fmt.Println(string(k))
			key, _ := numberfromb(k)
			fmt.Printf("%d,%s,%s,%x\n", key, k, v, v)
		}

		return nil
	})

	fmt.Println(err)
}

func TestName(t *testing.T) {
	// str := "account|AccountData.json"
	// fmt.Println(strings.Split(strings.Split(str, ".")[0], "|"))
	fmt.Println(common.Conf.BDB.Path, path.Dir(common.Conf.BDB.Path))
	pathstr, name := path.Split(common.Conf.BDB.Path)
	fmt.Println(path.Join(pathstr, "backup", time.Now().Format("20060102"), name))

}
