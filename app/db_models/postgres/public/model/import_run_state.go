//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package model

import (
	"github.com/google/uuid"
	"time"
)

type ImportRunState struct {
	ImportRunStatusID uuid.UUID `sql:"primary_key"`
	ImportRunID       uuid.UUID
	Status            ImportRunStatus
	CreatedAt         time.Time
	UpdatedAt         time.Time
}