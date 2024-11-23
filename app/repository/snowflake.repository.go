package repository

import (
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	"github.com/sahilsk11/fingest/app/domain"
	_ "github.com/snowflakedb/gosnowflake" // Snowflake driver
)

type SnowflakeRepository interface {
	// initializeConnection() error
	GetImportRun(importRunId uuid.UUID) (*domain.ImportRun, error)
}

type snowflakeRepositoryHandler struct {
	db *sql.DB
}

type SnowflakeCredentials struct {
	User     string
	Password string
	Account  string
	Schema   string
	Database string
}

func NewSnowflakeRepository(credentails SnowflakeCredentials) (SnowflakeRepository, error) {
	db, err := initializeConnection(credentails)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Snowflake connection: %w", err)
	}
	return &snowflakeRepositoryHandler{db: db}, nil
}

func initializeConnection(credentials SnowflakeCredentials) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@%s/%s/%s",
		credentials.User, credentials.Password, credentials.Account, credentials.Database, credentials.Schema)

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open Snowflake connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping Snowflake: %w", err)
	}

	return db, nil
}

func (h *snowflakeRepositoryHandler) GetImportRun(importRunId uuid.UUID) (*domain.ImportRun, error) {
	query := `
		SELECT 
			import_run_id,
			source_institution,
			account_type,
			data_type,
			file_source_format,
			table_name,
			s3_bucket,
			s3_path,
			file_name,
			created_at
		FROM fingest.public.import_run
		WHERE import_run_id = ?`

	row := h.db.QueryRow(query, importRunId.String())

	importRun := &domain.ImportRun{}
	err := row.Scan(
		&importRun.ImportRunID,
		&importRun.SourceInstitution,
		&importRun.AccountType,
		&importRun.DataType,
		&importRun.FileSourceFormat,
		&importRun.TableName,
		&importRun.S3Bucket,
		&importRun.S3Path,
		&importRun.FileName,
		&importRun.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("import run with id %s not found", importRunId.String())
		}
		return nil, fmt.Errorf("failed to fetch import run: %w", err)
	}

	return importRun, nil
}
