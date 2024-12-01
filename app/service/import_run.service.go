package service

import (
	"fmt"
	"time"

	"github.com/go-jet/jet/v2/postgres"
	"github.com/google/uuid"
	"github.com/sahilsk11/fingest/app/db_models/postgres/public/model"
	"github.com/sahilsk11/fingest/app/db_models/postgres/public/table"
	"github.com/sahilsk11/fingest/app/repository"
)

type ImportRunService interface {
	ImportFromSnowflake(importRunId uuid.UUID) error
	UpdateState(importRunId uuid.UUID, newStatus string, updatedAt time.Time, description *string) error
	ListUpdates(importRunId uuid.UUID) ([]model.ImportRunStatus, error)
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

func (h *importRunServiceHandler) ListUpdates(importRunId uuid.UUID) ([]model.ImportRunStatus, error) {
	results, err := h.ImportRunStateRepository.List(
		[]postgres.BoolExpression{
			table.ImportRunStatus.ImportRunID.EQ(postgres.UUID(importRunId)),
		},
		[]repository.SortOption{
			{
				Column:    table.ImportRunStatus.UpdatedAt,
				Direction: repository.Ascending,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list import run state: %w", err)
	}

	return results, nil
}
