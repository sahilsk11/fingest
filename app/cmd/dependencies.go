package cmd

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/sahilsk11/fingest/app/broker"
	"github.com/sahilsk11/fingest/app/repository"
	"github.com/sahilsk11/fingest/app/service"
	"github.com/sahilsk11/fingest/app/util"
)

type Dependencies struct {
	// repositories
	FileUploadRepository      repository.UploadedFileRepository
	S3Repository              repository.S3Repository
	IngestionRepository       repository.IngestionRepository
	ImportRunStatusRepository repository.ImportRunStatusRepository

	// services
	FileUploadService service.FileUploadService
	ImportRunService  service.ImportRunService

	// other
	Producer broker.Producer
}

func InitializeDependencies() (*Dependencies, error) {
	secrets, err := util.GetSecrets()
	if err != nil {
		return nil, fmt.Errorf("failed to get secrets: %w", err)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile("local"))
	if err != nil {
		return nil, fmt.Errorf("failed to load default aws config: %w", err)
	}
	cfg.Region = "us-east-1"

	db, err := util.NewConnection(secrets.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	producer, err := broker.NewProducer(secrets.Kafka.Host, secrets.Kafka.Port)
	if err != nil {
		return nil, fmt.Errorf("failed to create message producer: %w", err)
	}

	// repositories
	ingestionRepository := repository.NewAsynchronousIngestionRepository(producer)

	s3Repository, err := repository.NewS3Repository(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 repository: %w", err)
	}
	fileUploadRepository := repository.NewUploadedFileRepository(db)
	importRunStatusRepository := repository.NewImportRunStatusRepository(db)
	snowflakeRepository, err := repository.NewSnowflakeRepository(repository.SnowflakeCredentials{
		User:     secrets.Snowflake.User,
		Password: secrets.Snowflake.Password,
		Account:  secrets.Snowflake.Account,
		Schema:   secrets.Snowflake.Schema,
		Database: secrets.Snowflake.Database,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create snowflake repository: %w", err)
	}

	// services
	fileUploadService := service.NewFileUploadService(fileUploadRepository, s3Repository, ingestionRepository, importRunStatusRepository)
	importRunService := service.NewImportRunService(snowflakeRepository, importRunStatusRepository)

	return &Dependencies{
		// repositories
		FileUploadRepository:      fileUploadRepository,
		S3Repository:              s3Repository,
		IngestionRepository:       ingestionRepository,
		ImportRunStatusRepository: importRunStatusRepository,
		// services
		ImportRunService:  importRunService,
		FileUploadService: fileUploadService,
		// other
		Producer: producer,
	}, nil
}
