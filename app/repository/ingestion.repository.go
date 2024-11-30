package repository

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// we can move this to Kafka but for now, just use REST services to communicate with python backend

type IngestionRepository interface {
	NotifyFileUploaded(s3Bucket string, s3FilePath string) error
}

type ingestionRepositoryHandler struct {
	host   string
	client *http.Client
}

func NewIngestionRepository(host string, client *http.Client) IngestionRepository {
	return &ingestionRepositoryHandler{
		host:   host,
		client: client,
	}
}

func (h *ingestionRepositoryHandler) NotifyFileUploaded(s3Bucket string, s3FilePath string) error {
	// todo - add error handling/return value
	body := map[string]string{
		"s3Bucket":   s3Bucket,
		"s3FilePath": s3FilePath,
	}
	bodyJson, err := json.Marshal(body)
	if err != nil {
		return err
	}

	request, err := http.NewRequest(http.MethodPost, h.host+"/file-uploaded", bytes.NewBuffer(bodyJson))
	if err != nil {
		return fmt.Errorf("failed to create request for ingestion notify uploaded: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(request)
	if err != nil {
		return fmt.Errorf("failed to notify ingestion service of uploaded file: %w", err)
	}
	defer resp.Body.Close()

	return nil
}
