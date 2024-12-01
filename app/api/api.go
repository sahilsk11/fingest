package api

import (
	"context"

	"fmt"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/sahilsk11/fingest/app/cmd"
)

type ApiHandler struct {
	JwtDecodeToken  string
	AppDependencies cmd.Dependencies
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
		ctx.JSON(200, map[string]string{"message": "welcome to fingest"})
	})

	engine.POST("/upload", m.uploadFile)
	engine.POST("/list-import-run-updates", m.listImportRunUpdates)

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
