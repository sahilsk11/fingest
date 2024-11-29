package api

import (
	"context"
	"database/sql"

	"fmt"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/sahilsk11/fingest/app/service"
)

type ApiHandler struct {
	Db                *sql.DB
	FileUploadService service.FileUploadService
	JwtDecodeToken    string
}

func (m ApiHandler) InitializeRouterEngine(ctx context.Context) *gin.Engine {
	engine := gin.New()
	engine.Use(gin.Recovery())

	engine.Use(cors.New(cors.Config{
		AllowOrigins: []string{
			"http://localhost:5173",
		},
		AllowHeaders: []string{"Authorization", "Content-Type"},
	}))

	engine.GET("/", func(ctx *gin.Context) {
		ctx.JSON(200, map[string]string{"message": "welcome to alpha"})
	})

	engine.POST("/upload", m.uploadFile)

	return engine
}

func (m ApiHandler) StartApi(ctx context.Context, port int) error {
	engine := m.InitializeRouterEngine(ctx)
	return engine.Run(fmt.Sprintf(":%d", port))
}

func returnErrorJson(err error, c *gin.Context) {
	returnErrorJsonCode(err, c, 500)
}

func returnErrorJsonCode(err error, c *gin.Context, code int) {
	// lg := logger.FromContext(c)
	// lg.Errorf("[%d] %s", code, err.Error())
	c.AbortWithStatusJSON(code, gin.H{
		"error": err.Error(),
	})
}
