package api

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type listImportRunUpdatesRequest struct {
	ImportRunId string `json:"importRunId"`
}

type listImportRunUpdatesResponse struct {
	Updates []ImportRunUpdate `json:"updates"`
}

type ImportRunUpdate struct {
	Status      string  `json:"status"`
	Description *string `json:"description"`
	UpdatedAt   string  `json:"updatedAt"`
}

func (m ApiHandler) listImportRunUpdates(c *gin.Context) {
	var requestBody listImportRunUpdatesRequest
	if err := c.ShouldBindJSON(&requestBody); err != nil {
		returnErrorJson(err, c)
		return
	}

	importRunId, err := uuid.Parse(requestBody.ImportRunId)
	if err != nil {
		returnErrorJson(fmt.Errorf("failed to parse import run id: %w", err), c)
		return
	}

	results, err := m.AppDependencies.ImportRunService.ListUpdates(importRunId)
	if err != nil {
		returnErrorJson(err, c)
		return
	}

	updates := []ImportRunUpdate{}
	for _, result := range results {
		updates = append(updates, ImportRunUpdate{
			Status:      result.Status,
			Description: result.Description,
			UpdatedAt:   result.UpdatedAt.String(),
		})
	}

	c.JSON(200, listImportRunUpdatesResponse{
		Updates: updates,
	})
}
