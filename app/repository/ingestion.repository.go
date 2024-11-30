package repository

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
)

// we can move this to Kafka but for now, just use REST services to communicate with python backend

type IngestionRepository interface {
	NotifyFileUploaded(s3Bucket string, s3FilePath string, sourceInstition string) (*NotifyFileUploadedResponse, error)
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

type notifyFileUploadedRequest struct {
	S3Bucket          string `json:"s3Bucket"`
	S3FilePath        string `json:"s3FilePath"`
	SourceInstitution string `json:"sourceInstitution"`
}

type NotifyFileUploadedResponse struct {
	ImportRunID uuid.UUID `json:"importRunId"`
}

func (h *ingestionRepositoryHandler) NotifyFileUploaded(s3Bucket string, s3FilePath string, sourceInstitution string) (*NotifyFileUploadedResponse, error) {
	// todo - add error handling/return value
	body := notifyFileUploadedRequest{
		S3Bucket:          s3Bucket,
		S3FilePath:        s3FilePath,
		SourceInstitution: sourceInstitution,
	}
	bodyJson, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequest(http.MethodPost, h.host+"/file-uploaded", bytes.NewBuffer(bodyJson))
	if err != nil {
		return nil, fmt.Errorf("failed to create request for ingestion notify uploaded: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("failed to notify ingestion service of uploaded file: %w", err)
	}
	defer resp.Body.Close()

	var response NotifyFileUploadedResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ingestion notify uploaded response: %w", err)
	}

	return &response, nil
}
