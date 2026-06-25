package common

import (
	"cc_template/command"
	"embed"
	"fmt"
	"sync"

	"github.com/clakeboy/golib/components"
	"github.com/clakeboy/golib/components/snowflake"
	"github.com/clakeboy/storm-rev"
)

var HtmlFiles embed.FS
var Conf *Config
var BDB *storm.DB
var MemCache *components.MemCache
var SnowFlake *snowflake.SnowFlake
var DBMap = make(map[string]*storm.DB)
var dbMu sync.RWMutex

//var debugLog = components.NewSysLog("debug_")

func DebugF(str string, args ...interface{}) {
	if command.CmdDebug {
		fmt.Printf("[DEBUG] "+str+"\n", args...)
	}
}

// GetDB 并发安全地获取数据库连接，如果是首次访问，则动态打开并缓存
func GetDB(dbName string) *storm.DB {
	if dbName == "" {
		return BDB
	}

	// 1. 读锁检查是否已存在
	dbMu.RLock()
	db, ok := DBMap[dbName]
	dbMu.RUnlock()
	if ok {
		return db
	}

	// 2. 写锁进行初始化
	dbMu.Lock()
	defer dbMu.Unlock()

	// 二次检查，防止并发冲突
	if db, ok = DBMap[dbName]; ok {
		return db
	}

	var dbPath string
	if Conf != nil && Conf.BDB != nil && Conf.BDB.List != nil {
		if path, ok := Conf.BDB.List[dbName]; ok {
			dbPath = path
		} else {
			// 模糊匹配
			for k, path := range Conf.BDB.List {
				if k == dbName || path == dbName {
					dbPath = path
					dbName = k
					break
				}
			}
		}
	}

	if dbPath != "" {
		// 检查路径是否已经缓存了 DB（避免同一文件重复打开）
		if db, ok = DBMap[dbPath]; ok {
			DBMap[dbName] = db
			return db
		}

		var err error
		db, err = storm.Open(dbPath)
		if err != nil {
			panic(fmt.Sprintf("动态打开数据库失败: %v, 简称: %s, 路径: %s", err, dbName, dbPath))
		}
		DBMap[dbName] = db
		DBMap[dbPath] = db

		return db
	}

	// 若未配置，默认回退
	if dbName == "sys" {
		return BDB
	}
	return BDB
}
