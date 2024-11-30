package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/sahilsk11/fingest/app/api"
	"github.com/sahilsk11/fingest/app/broker"
	"github.com/sahilsk11/fingest/app/repository"
	"github.com/sahilsk11/fingest/app/service"
	"github.com/sahilsk11/fingest/app/util"

	_ "github.com/lib/pq"
)

func main() {
	secrets, err := util.GetSecrets()
	if err != nil {
		log.Fatal(err)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile("local"))
	if err != nil {
		log.Fatal(err)
	}
	cfg.Region = "us-east-1"
	db, err := util.NewConnection(secrets.Database)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal(err)
	}
	producer, err := broker.NewProducer(secrets.Kafka.Host, secrets.Kafka.Port)
	if err != nil {
		log.Fatal(err)
	}

	ingestionRepository := repository.NewAsynchronousIngestionRepository(producer)

	s3Repository, err := repository.NewS3Repository(cfg)
	if err != nil {
		panic(err)
	}
	fileUploadRepository := repository.NewUploadedFileRepository(db)
	importRunStateRepository := repository.NewImportRunStateRepository(db)
	fileUploadService := service.NewFileUploadService(fileUploadRepository, s3Repository, ingestionRepository, importRunStateRepository)

	apiHandler := api.ApiHandler{
		FileUploadService: fileUploadService,
	}

	ctx := context.Background()
	apiHandler.StartApi(ctx, 3010)
}
