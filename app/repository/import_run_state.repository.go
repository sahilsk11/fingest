package repository

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/google/uuid"
	"github.com/sahilsk11/fingest/app/db_models/postgres/public/model"
	"github.com/sahilsk11/fingest/app/db_models/postgres/public/table"
	_ "github.com/snowflakedb/gosnowflake" // Snowflake driver
)

type ImportRunStateRepository interface {
	Get(importRunId uuid.UUID) (*model.ImportRunState, error)
}

type importRunStateRepositoryHandler struct {
	db *sql.DB
}

func NewImportRunStateRepository(db *sql.DB) ImportRunStateRepository {
	return &importRunStateRepositoryHandler{db: db}
}

func (h *importRunStateRepositoryHandler) Get(importRunId uuid.UUID) (*model.ImportRunState, error) {
	t := table.ImportRunState
	query := t.SELECT(t.AllColumns).
		WHERE(
			t.ImportRunID.EQ(postgres.UUID(importRunId)),
		)

	out := model.ImportRunState{}
	err := query.Query(h.db, &out)
	if err != nil && errors.Is(err, qrm.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch import run state: %w", err)
	}

	return &out, nil
}
