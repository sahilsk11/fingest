package repository

import (
	"database/sql"

	"github.com/sahilsk11/fingest/app/db_models/postgres/public/model"
	"github.com/sahilsk11/fingest/app/db_models/postgres/public/table"
)

type UploadedFileRepository interface {
	Add(model.UploadedFile) (*model.UploadedFile, error)
}

type uploadedFileRepositoryHandler struct {
	db *sql.DB
}

// Add function to insert a new uploaded file record
func (r *uploadedFileRepositoryHandler) Add(uploadedFile model.UploadedFile) (*model.UploadedFile, error) {
	t := table.UploadedFile
	query := t.INSERT(t.MutableColumns).MODEL(uploadedFile).RETURNING(t.AllColumns)
	out := model.UploadedFile{}
	err := query.Query(r.db, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}