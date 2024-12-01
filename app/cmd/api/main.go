package main

import (
	"context"
	"fmt"
	"log"

	"github.com/sahilsk11/fingest/app/api"
	"github.com/sahilsk11/fingest/app/cmd"

	_ "github.com/lib/pq"
)

func main() {
	appDependencies, err := cmd.InitializeDependencies()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to initialize dependencies: %w", err))
	}

	apiHandler := api.ApiHandler{
		AppDependencies: *appDependencies,
	}

	ctx := context.Background()
	apiHandler.StartApi(ctx, 3010)
}
