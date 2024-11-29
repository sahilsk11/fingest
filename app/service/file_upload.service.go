package service

import "github.com/sahilsk11/fingest/app/repository"

const MAX_FILE_SIZE_KB = 1024 * 1024 * 10

type FileUploadService interface {
	UploadFile(filename string, fileBytes []byte, fileSizeKb int) error
}

type fileUploadServiceHandler struct {
	UploadedFileRepository repository.UploadedFileRepository
}

func NewFileUploadService() FileUploadService {
	return &fileUploadServiceHandler{}
}

func (h *fileUploadServiceHandler) UploadFile(filename string, fileBytes []byte, fileSizeKb int) error {
	return nil
}
