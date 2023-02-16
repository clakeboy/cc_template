package common

import (
	"github.com/clakeboy/golib/ckdb"
	"gopkg.in/yaml.v3"
	"os"
)

// Config 总配置结构
type Config struct {
	path      string           `json:"-" yaml:"-"`
	System    *SystemConfig    `json:"system" yaml:"system"`
	DB        *ckdb.DBConfig   `json:"db" yaml:"db"`
	BDB       *BoltDBConfig    `json:"boltdb" yaml:"boltdb"`
	Cookie    *CookieConfig    `json:"cookie" yaml:"cookie"`
	HttpProxy *HttpProxyConfig `json:"http_proxy" yaml:"http_proxy"`
}

// SystemConfig HTTP及系统配置
type SystemConfig struct {
	Port string `json:"port" yaml:"port"`
	Ip   string `json:"ip" yaml:"ip"`
	Pid  string `json:"pid" yaml:"pid"`
}

// CookieConfig Cookie 配置
type CookieConfig struct {
	Path     string `json:"path" yaml:"path"`
	Domain   string `json:"domain" yaml:"domain"`
	Source   bool   `json:"source" yaml:"source"`
	HttpOnly bool   `json:"http_only" yaml:"http_only"`
}

// BoltDBConfig boltdb 配置
type BoltDBConfig struct {
	Path string `json:"path" yaml:"path"`
}

// HttpProxyConfig HTTP代理信息
type HttpProxyConfig struct {
	Use  bool   `json:"use" yaml:"use"`   //是否使用代理
	Addr string `json:"addr" yaml:"addr"` //HTTP代理地址
}

var defConfData = `# 系统配置
system:
  port: "12352"
  ip: ""
  pid: cc_template.pid
# boltdb 本地缓存数据库配置
boltdb:
  path: ./db/sys.db
# cookie 配置
cookie:
  path: /
  domain: ""
  source: false
  http_only: false`

// NewYamlConfig 读取一个YAML配置文件
func NewYamlConfig(confFile string) *Config {
	data, err := os.ReadFile(confFile)
	if err != nil {
		data = []byte(defConfData)
		SaveYamlConfig(confFile, data)
	}

	var conf Config
	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		panic(err)
	}
	return &conf
}

// SaveYamlConfig 写入YAML配置
func SaveYamlConfig(fileName string, content []byte) {
	_ = os.WriteFile(fileName, content, 0755)
}
