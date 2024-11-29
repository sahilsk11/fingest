package repository

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const S3_BUCKET_NAME = "fingest-uploaded-files"

type S3Repository interface {
	UploadFile(ctx context.Context, fileName string, fileBytes []byte, fileSizeKb int) (*string, error)
}

type s3RepositoryHandler struct {
	s3Client *s3.Client
}

func NewS3Repository(sdkConfig aws.Config) (S3Repository, error) {
	return &s3RepositoryHandler{
		s3Client: s3.NewFromConfig(sdkConfig),
	}, nil
}

func (h *s3RepositoryHandler) UploadFile(ctx context.Context, fileName string, fileBytes []byte, fileSizeKb int) (*string, error) {
	bucket := S3_BUCKET_NAME
	key := fileName
	_, err := h.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(fileBytes),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to upload file to S3: %w", err)
	}

	return &key, nil
}
