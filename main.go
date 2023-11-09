package main

//./sys-monitor_darwin --debug --server --config=../dev.conf
import (
	"cc_template/command"
	"cc_template/common"
	"cc_template/router"
	"fmt"
	"os"
	"path"

	"github.com/asdine/storm/v3"
	"github.com/clakeboy/golib/components"
	"github.com/clakeboy/golib/utils"
)

var sigs chan os.Signal
var done chan bool
var (
	AppName      string //应用名称
	AppVersion   string //应用名称
	BuildVersion string //编译版本
	BuildTime    string //编译时间
	GitRevision  string //Git 版本
	GitBranch    string //Git 分支
	GoVersion    string //Golang
)

// var templateFiles embed.FS
//
//go:embed assets/templates/*
var httpServer *router.HttpServer

//go:embed assets/html/*
// var htmlFiles embed.FS

func main() {
	initService()
	go utils.ExitApp(sigs, func(s os.Signal) {
		_ = os.Remove(command.CmdPidName)
		done <- true
	})
	httpServer.Start()
}

func initService() {
	var err error
	command.InitCommand()
	if command.CmdShowVersion {
		Version()
		os.Exit(0)
	}
	//获取YAML
	common.Conf = common.NewYamlConfig(command.CmdConfFile)

	//写入PID文件
	if common.Conf.System.Pid != "" {
		command.CmdPidName = common.Conf.System.Pid
	}
	utils.WritePid(command.CmdPidName)
	//初始化关闭信号
	sigs = make(chan os.Signal, 1)
	done = make(chan bool, 1)
	//初始化全局内存缓存
	common.MemCache = components.NewMemCache()
	//初始化全局snowid生成器
	//common.SnowFlake, err = snowflake.NewShowFlake(1, 1, 1)
	//if err != nil {
	//	fmt.Println("初始化 SnowId 错误：", err)
	//	return
	//}

	//初始化BDB微型数据库
	if !utils.PathExists(path.Dir(common.Conf.BDB.Path)) {
		_ = os.MkdirAll(path.Dir(common.Conf.BDB.Path), 0775)
	}
	common.BDB, err = storm.Open(common.Conf.BDB.Path)
	if err != nil {
		fmt.Println("open storm database error:", err)
		os.Exit(-1)
	}

	//初始化HTTP WEB服务
	httpServer = router.NewHttpServer(common.Conf.System.Ip+":"+common.Conf.System.Port, command.CmdDebug, command.CmdCross, command.CmdPProf)

	//初始化模板文件
	if command.CmdDebug {
		// httpServer.LoadTemplate("./assets/templates/**/*")
		httpServer.StaticFs("./assets/html")
	} else {
		// httpServer.TemplateEmbedFS(templateFiles, "assets/templates/**/*")
		// httpServer.StaticEmbedFS(htmlFiles)
	}
}

func Version() {
	fmt.Println("App Name:", AppName)
	fmt.Println("App Version:", AppVersion)
	fmt.Println("Build Version:", BuildVersion)
	fmt.Println("Build Time:", BuildTime)
	fmt.Println("Git Revision:", GitRevision)
	fmt.Println("Git Branch:", GitBranch)
	fmt.Println("Golang Version:", GoVersion)
}
