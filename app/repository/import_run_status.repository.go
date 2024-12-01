package repository

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/google/uuid"
	"github.com/sahilsk11/fingest/app/db_models/postgres/public/model"
	"github.com/sahilsk11/fingest/app/db_models/postgres/public/table"
	_ "github.com/snowflakedb/gosnowflake" // Snowflake driver
)

type ImportRunStatusRepository interface {
	Get(importRunId uuid.UUID) (*model.ImportRunStatus, error)
	Create(m model.ImportRunStatus) (*model.ImportRunStatus, error)
}

type ImportRunStatusRepositoryHandler struct {
	db *sql.DB
}

func NewImportRunStatusRepository(db *sql.DB) ImportRunStatusRepository {
	return &ImportRunStatusRepositoryHandler{db: db}
}

func (h *ImportRunStatusRepositoryHandler) Get(importRunId uuid.UUID) (*model.ImportRunStatus, error) {
	t := table.ImportRunStatus
	query := t.SELECT(t.AllColumns).
		WHERE(
			t.ImportRunID.EQ(postgres.UUID(importRunId)),
		)

	out := model.ImportRunStatus{}
	err := query.Query(h.db, &out)
	if err != nil && errors.Is(err, qrm.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch import run state: %w", err)
	}

	return &out, nil
}

func (h *ImportRunStatusRepositoryHandler) Create(m model.ImportRunStatus) (*model.ImportRunStatus, error) {
	m.CreatedAt = time.Now().UTC()
	// this is confusing, but the timestamp here should be the time
	// the event occurred
	// m.UpdatedAt = time.Now().UTC()
	t := table.ImportRunStatus
	query := t.INSERT(t.MutableColumns).MODEL(m).RETURNING(t.AllColumns)
	out := model.ImportRunStatus{}
	err := query.Query(h.db, &out)
	if err != nil {
		return nil, fmt.Errorf("failed to create import run state: %w", err)
	}

	return &out, nil
}
