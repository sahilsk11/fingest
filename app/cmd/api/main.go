package main

import (
	"context"

	"github.com/sahilsk11/fingest/app/api"
	"github.com/sahilsk11/fingest/app/service"
)

func main() {
	fileUploadService := service.NewFileUploadService()
	apiHandler := api.ApiHandler{
		FileUploadService: fileUploadService,
	}

	ctx := context.Background()
	apiHandler.StartApi(ctx, 3010)
}
