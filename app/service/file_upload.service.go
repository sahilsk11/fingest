package service

import (
	"context"

	"github.com/sahilsk11/fingest/app/repository"
)

const MAX_FILE_SIZE_KB = 1024 * 1024 * 10

type FileUploadService interface {
	UploadFile(filename string, fileBytes []byte, fileSizeKb int) error
}

type fileUploadServiceHandler struct {
	UploadedFileRepository repository.UploadedFileRepository
	S3Repository           repository.S3Repository
}

func NewFileUploadService(
	uploadedFileRepository repository.UploadedFileRepository,
	s3Repository repository.S3Repository,
) FileUploadService {
	return &fileUploadServiceHandler{
		UploadedFileRepository: uploadedFileRepository,
		S3Repository:           s3Repository,
	}
}

func (h *fileUploadServiceHandler) UploadFile(filename string, fileBytes []byte, fileSizeKb int) error {
	_, err := h.S3Repository.UploadFile(context.Background(), filename, fileBytes, fileSizeKb)
	if err != nil {
		return err
	}
	return nil
}
