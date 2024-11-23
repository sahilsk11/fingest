package domain

import (
	"time"

	"github.com/google/uuid"
)

// keep in sync with ingestion.baml

type AccountType string

const (
	BANK            AccountType = "BANK"
	BROKERAGE       AccountType = "BROKERAGE"
	CREDIT_CARD     AccountType = "CREDIT_CARD"
	CRYPTO_EXCHANGE AccountType = "CRYPTO_EXCHANGE"
	OTHER           AccountType = "OTHER"
)

type DataType string

const (
	TRANSACTION DataType = "TRANSACTION"
	POSITION    DataType = "POSITION"
	OPENLOT     DataType = "OPENLOT"
	BALANCES    DataType = "BALANCES"
)

type FileSourceFormat string

const (
	CSV   FileSourceFormat = "CSV"
	JSON  FileSourceFormat = "JSON"
	PDF   FileSourceFormat = "PDF"
	EMAIL FileSourceFormat = "EMAIL"
)

type ImportRun struct {
	ImportRunID       uuid.UUID
	SourceInstitution string
	AccountType       AccountType
	DataType          DataType
	FileSourceFormat  FileSourceFormat
	TableName         string
	S3Bucket          *string
	S3Path            *string
	FileName          *string
	CreatedAt         time.Time
}
