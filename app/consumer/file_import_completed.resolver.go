package consumer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type fileImportCompletedRequest struct {
	ImportRunId string  `json:"importRunId"`
	Status      string  `json:"status"`
	Description *string `json:"description"`
}

func (m consumerHandler) fileImportCompleted(payloadBytes []byte, ts time.Time) error {
	payload := fileImportCompletedRequest{}
	err := json.Unmarshal(payloadBytes, &payload)
	if err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	importRunIdStr := payload.ImportRunId
	importRunId, err := uuid.Parse(importRunIdStr)
	if err != nil {
		return fmt.Errorf("failed to parse import run id: %w", err)
	}

	newStatus := payload.Status
	err = m.appDependencies.ImportRunService.UpdateState(importRunId, newStatus, ts, payload.Description)
	if err != nil {
		return err
	}

	return nil
}
