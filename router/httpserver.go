package router

import (
	"cc_template/middles"
	"embed"
	"fmt"
	"html/template"

	"github.com/clakeboy/golib/components"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
)

type HttpServer struct {
	server  *gin.Engine
	isDebug bool
	isCross bool
	addr    string
}

func NewHttpServer(addr string, isDebug bool, isCross, isPProf bool) *HttpServer {
	server := &HttpServer{isCross: isCross, isDebug: isDebug, addr: addr}
	server.Init()
	if isPProf {
		server.StartPprof()
	}
	return server
}

func (h *HttpServer) Start() {
	wait := make(chan bool)
	go func() {
		err := h.server.Run(h.addr)
		if err != nil {
			wait <- true
		}
	}()
	<-wait
}

func (h *HttpServer) Init() {
	if h.isDebug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	h.server = gin.New()

	//使用中间件
	if h.isDebug {
		h.server.Use(gin.Logger(), gin.Recovery())
	} else {
		h.server.Use(middles.Logger(), middles.Recovery())
	}

	h.server.Use(middles.Cache())
	h.server.Use(middles.BoltDatabase())
	h.server.Use(middles.Cookie())
	h.server.Use(middles.StaticCache())
	//h.server.Use(middles.Mongo())
	//h.server.Use(middles.Redis())
	//h.server.Use(gzip.Gzip(gzip.DefaultCompression))
	//h.server.Use(middles.Session())
	h.server.Use(gzip.Gzip(gzip.DefaultCompression, gzip.WithExcludedExtensions([]string{".pdf", ".mp4"})))
	//跨域调用的OPTIONS
	h.server.OPTIONS("*action", func(c *gin.Context) {
		components.Cross(c, h.isCross, c.Request.Header.Get("Origin"))
	})

	//POST服务接收
	h.server.POST("/serv/:controller/:action", func(c *gin.Context) {
		components.Cross(c, h.isCross, c.Request.Header.Get("Origin"))
		controller := GetController(c.Param("controller"), c)
		if controller == nil {
			fmt.Println("not found conntroller")
			return
		}
		components.CallAction(controller, c)
	})
	//GET服务
	h.server.GET("/front/:controller/:action", func(c *gin.Context) {
		controller := GetController(c.Param("controller"), c)
		components.CallActionGet(controller, c)
	})

	////静态文件访问
	//h.server.GET("/backstage/:filepath", func(c *gin.Context) {
	//	c.FileFromFS("",h.embed)
	//})

	//静态文件API接口
	//h.server.Static("/backstage", "./assets/html")
	// h.server.SetFuncMap(template.FuncMap{
	// 	"format_date": controllers.FormatDate,
	// 	"cond_str":    controllers.YN,
	// 	"show_bg":     controllers.ShowBg,
	// 	"show_vt":     controllers.ShowVt,
	// })
}

// StartPprof 启动性能探测
func (h *HttpServer) StartPprof() {
	components.InitPprof(h.server)
}

// StaticEmbedFS 设置静态文件目录为打包文件
func (h *HttpServer) StaticEmbedFS(fs embed.FS) {
	h.server.StaticFS("/static", &middles.EmbedFiles{
		Embed: fs,
		Path:  "assets/html",
	})
}

// StaticFs 设置静态文件目录
func (h *HttpServer) StaticFs(pathStr string) {
	h.server.Static("/static", pathStr)
}

// TemplateEmbedFS 设置模板文件目录为打包文件
func (h *HttpServer) TemplateEmbedFS(fs embed.FS, patterns string) {
	tmpl := template.New("").Funcs(h.server.FuncMap)
	tmpl = template.Must(tmpl.ParseFS(fs, patterns))
	h.server.SetHTMLTemplate(tmpl)
}

// LoadTemplate 设置模板目录目录
func (h *HttpServer) LoadTemplate(pathStr string) {
	//模板页
	h.server.LoadHTMLGlob(pathStr)
}
