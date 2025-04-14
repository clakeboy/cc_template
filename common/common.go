package common

import (
	"cc_template/command"
	"embed"
	"fmt"

	"github.com/clakeboy/golib/components"
	"github.com/clakeboy/golib/components/snowflake"
	"github.com/clakeboy/storm-rev"
)

var HtmlFiles embed.FS
var Conf *Config
var BDB *storm.DB
var MemCache *components.MemCache
var SnowFlake *snowflake.SnowFlake

//var debugLog = components.NewSysLog("debug_")

func DebugF(str string, args ...interface{}) {
	if command.CmdDebug {
		fmt.Printf("[DEBUG] "+str+"\n", args...)
	}
}
