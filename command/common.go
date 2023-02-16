package command

import (
	"flag"
	"fmt"
	"os"
)

var (
	CmdDebug       bool
	CmdCross       bool
	CmdPProf       bool
	CmdConfFile    string
	CmdPidName     string
	CmdShowVersion bool
)

func InitCommand() {
	flag.BoolVar(&CmdDebug, "debug", false, "is runtime debug mode")
	flag.BoolVar(&CmdCross, "cross", false, "use cross request")
	flag.BoolVar(&CmdPProf, "pprof", false, "open go pprof debug")
	flag.StringVar(&CmdConfFile, "config", "./main.conf", "app config file")
	flag.StringVar(&CmdPidName, "pid", "./cc_template.pid", "app config file")
	flag.BoolVar(&CmdShowVersion, "version", false, "show this version information")
	flag.Parse()
	ExecCommand()
}

func ExecCommand() {

}

// 结束程序
func Exit(msg string) {
	fmt.Println(msg)
	os.Exit(1)
}
