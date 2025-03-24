package middles

import (
	"github.com/gin-gonic/gin"
	"path"
)

var AllowFileExt = map[string]bool{
	".svg": true,
	".js":  true,
	".css": true,
	".jpg": true,
	".gif": true,
	".png": true,
}

func StaticCache(extList ...string) gin.HandlerFunc {
	if extList != nil && len(extList) > 0 {
		for _, v := range extList {
			AllowFileExt[v] = true
		}
	}
	return StaticMiddle
}

func StaticMiddle(c *gin.Context) {
	extStr := path.Ext(c.Request.URL.RequestURI())
	if _, ok := AllowFileExt[extStr]; ok {
		c.Header("Cache-Control", "public, max-age=31536000")
	}
	c.Next()
	//fmt.Println(c.Writer.Header().Get("Content-Type"))
}
