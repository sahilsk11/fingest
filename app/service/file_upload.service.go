package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/sahilsk11/fingest/app/db_models/postgres/public/model"
	"github.com/sahilsk11/fingest/app/repository"
)

const MAX_FILE_SIZE_KB = 1024 * 1024 * 10

type FileUploadService interface {
	UploadFile(filename string, fileBytes []byte, fileSizeKb int) error
}

type fileUploadServiceHandler struct {
	UploadedFileRepository repository.UploadedFileRepository
	S3Repository           repository.S3Repository
	IngestionRepository    repository.IngestionRepository
}

func NewFileUploadService(
	uploadedFileRepository repository.UploadedFileRepository,
	s3Repository repository.S3Repository,
	ingestionRepository repository.IngestionRepository,
) FileUploadService {
	return &fileUploadServiceHandler{
		UploadedFileRepository: uploadedFileRepository,
		S3Repository:           s3Repository,
		IngestionRepository:    ingestionRepository,
	}
}

func (h *fileUploadServiceHandler) UploadFile(filename string, fileBytes []byte, fileSizeKb int) error {
	filePath, err := h.S3Repository.UploadFile(context.Background(), filename, fileBytes, fileSizeKb)
	if err != nil {
		return fmt.Errorf("failed to upload file to S3: %w", err)
	}

	userId := uuid.MustParse("761e3e29-c372-4e41-be6c-9475d53c642b")

	uploadedFile, err := h.UploadedFileRepository.Add(model.UploadedFile{
		UploadedFileID:   [16]byte{},
		UploadedByUserID: userId,
		FileName:         filename,
		S3Bucket:         repository.S3_BUCKET_NAME,
		S3FilePath:       *filePath,
		FileSizeKb:       int32(fileSizeKb),
	})
	if err != nil {
		return fmt.Errorf("failed to add uploaded file to db: %w", err)
	}

	err = h.IngestionRepository.NotifyFileUploaded(uploadedFile.S3Bucket, uploadedFile.S3FilePath)
	if err != nil {
		return fmt.Errorf("failed to notify ingestion service of uploaded file: %w", err)
	}

	return nil
}
