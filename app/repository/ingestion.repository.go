package repository

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/sahilsk11/fingest/app/broker"
	"github.com/sahilsk11/fingest/app/domain"
)

// we can move this to Kafka but for now, just use REST services to communicate with python backend

type IngestionRepository interface {
	NotifyFileUploaded(importRunId uuid.UUID, s3Bucket string, s3FilePath string, sourceInstitution string, outputFormat domain.TransformerOutputSchema) (*NotifyFileUploadedResponse, error)
}

type ingestionRepositoryRestHandler struct {
	host   string
	client *http.Client
}

type ingestionRepositoryBrokerHandler struct {
	Producer broker.Producer
}

func NewSynchronousIngestionRepository(host string, client *http.Client) IngestionRepository {
	return &ingestionRepositoryRestHandler{
		host:   host,
		client: client,
	}
}

func NewAsynchronousIngestionRepository(producer broker.Producer) IngestionRepository {
	return &ingestionRepositoryBrokerHandler{
		Producer: producer,
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

func (h *ingestionRepositoryRestHandler) NotifyFileUploaded(importRunId uuid.UUID, s3Bucket string, s3FilePath string, sourceInstitution string, outputFormat domain.TransformerOutputSchema) (*NotifyFileUploadedResponse, error) {
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

func (h *ingestionRepositoryBrokerHandler) NotifyFileUploaded(importRunId uuid.UUID, s3Bucket string, s3FilePath string, sourceInstitution string, outputFormat domain.TransformerOutputSchema) (*NotifyFileUploadedResponse, error) {
	// todo - add error handling/return value
	payload := map[string]interface{}{
		"s3Bucket":          s3Bucket,
		"s3FilePath":        s3FilePath,
		"sourceInstitution": sourceInstitution,
		"importRunId":       importRunId,
		"outputSchema":      outputFormat,
	}
	err := h.Producer.Publish("FILE_UPLOADED", payload)
	if err != nil {
		return nil, fmt.Errorf("failed to publish ingestion notify uploaded event: %w", err)
	}
	return &NotifyFileUploadedResponse{}, nil
}
