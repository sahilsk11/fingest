package service

import "github.com/google/uuid"

type ImportRunService interface{}

type importRunServiceHandler struct{}

func NewImportRunService() ImportRunService {
	return &importRunServiceHandler{}
}

func (h *importRunServiceHandler) ImportFromSnowflake(importRunId uuid.UUID) error {
	return nil
}
