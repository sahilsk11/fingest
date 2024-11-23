package service

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/sahilsk11/fingest/app/db_models/postgres/public/model"
	"github.com/sahilsk11/fingest/app/repository"
	"github.com/sahilsk11/fingest/app/util"
)

type ImportRunService interface {
	ImportFromSnowflake(importRunId uuid.UUID) error
}

type importRunServiceHandler struct {
	SnowflakeRepository      repository.SnowflakeRepository
	ImportRunStateRepository repository.ImportRunStateRepository
}

func NewImportRunService(
	snowflakeRepository repository.SnowflakeRepository,
	importRunStateRepository repository.ImportRunStateRepository,
) ImportRunService {
	return &importRunServiceHandler{
		SnowflakeRepository:      snowflakeRepository,
		ImportRunStateRepository: importRunStateRepository,
	}
}

func (h *importRunServiceHandler) ImportFromSnowflake(importRunId uuid.UUID) error {
	existingState, err := h.ImportRunStateRepository.Get(importRunId)
	if err != nil {
		return fmt.Errorf("failed to get import run state with id %s: %w", importRunId.String(), err)
	}
	if existingState != nil && existingState.Status == model.ImportRunStatus_Completed {
		return nil
	}

	importRun, err := h.SnowflakeRepository.GetImportRun(importRunId)
	if err != nil {
		return fmt.Errorf("failed to get import run with id %s: %w", importRunId.String(), err)
	}

	columns, data, err := h.SnowflakeRepository.GetTableData(importRun.TableName)
	if err != nil {
		return fmt.Errorf("failed to get table data for import run with id %s: %w", importRunId.String(), err)
	}

	util.Pprint(columns)
	util.Pprint(data)

	return nil
}
