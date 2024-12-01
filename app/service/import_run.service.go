package service

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sahilsk11/fingest/app/db_models/postgres/public/model"
	"github.com/sahilsk11/fingest/app/repository"
)

type ImportRunService interface {
	ImportFromSnowflake(importRunId uuid.UUID) error
	UpdateState(importRunId uuid.UUID, newStatus string, updatedAt time.Time, description *string) error
}

type importRunServiceHandler struct {
	SnowflakeRepository      repository.SnowflakeRepository
	ImportRunStateRepository repository.ImportRunStatusRepository
}

func NewImportRunService(
	snowflakeRepository repository.SnowflakeRepository,
	importRunStateRepository repository.ImportRunStatusRepository,
) ImportRunService {
	return &importRunServiceHandler{
		SnowflakeRepository:      snowflakeRepository,
		ImportRunStateRepository: importRunStateRepository,
	}
}

func (h *importRunServiceHandler) ImportFromSnowflake(importRunId uuid.UUID) error {
	// existingState, err := h.ImportRunStateRepository.Get(importRunId)
	// if err != nil {
	// 	return fmt.Errorf("failed to get import run state with id %s: %w", importRunId.String(), err)
	// }
	// if existingState != nil && existingState.Status == model.ImportRunStatus_Completed {
	// 	return nil
	// }

	importRun, err := h.SnowflakeRepository.GetImportRun(importRunId)
	if err != nil {
		return fmt.Errorf("failed to get import run with id %s: %w", importRunId.String(), err)
	}

	// get the raw rows from the given table; most likely a dynamic data table
	_, _, err = h.SnowflakeRepository.GetTableData(importRun.TableName)
	if err != nil {
		return fmt.Errorf("failed to get table data for import run with id %s: %w", importRunId.String(), err)
	}

	return nil
}

func (h *importRunServiceHandler) UpdateState(importRunId uuid.UUID, newStatus string, updatedAt time.Time, description *string) error {
	_, err := h.ImportRunStateRepository.Create(model.ImportRunStatus{
		ImportRunID: importRunId,
		UpdatedAt:   updatedAt,
		Status:      newStatus,
		Description: description,
	})
	if err != nil {
		return fmt.Errorf("failed to update import run state with id %s: %w", importRunId.String(), err)
	}

	return nil
}
