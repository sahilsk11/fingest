package repository

import (
	"database/sql"
	"time"

	"github.com/sahilsk11/fingest/app/db_models/postgres/public/model"
	"github.com/sahilsk11/fingest/app/db_models/postgres/public/table"
)

type UploadedFileRepository interface {
	Add(model.UploadedFile) (*model.UploadedFile, error)
}

type uploadedFileRepositoryHandler struct {
	db *sql.DB
}

func NewUploadedFileRepository(db *sql.DB) UploadedFileRepository {
	return &uploadedFileRepositoryHandler{db: db}
}

// Add function to insert a new uploaded file record
func (r *uploadedFileRepositoryHandler) Add(uploadedFile model.UploadedFile) (*model.UploadedFile, error) {
	uploadedFile.CreatedAt = time.Now().UTC()

	t := table.UploadedFile
	query := t.INSERT(t.MutableColumns).MODEL(uploadedFile).RETURNING(t.AllColumns)
	out := model.UploadedFile{}
	err := query.Query(r.db, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}
