package api

import (
	"io"

	"github.com/gin-gonic/gin"
)

func (m ApiHandler) uploadFile(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		returnErrorJson(err, c)
		return
	}

	fileSizeKb := file.Size / 1024

	fileData, err := file.Open()
	if err != nil {
		returnErrorJson(err, c)
		return
	}

	fileBytes, err := io.ReadAll(fileData)
	if err != nil {
		returnErrorJson(err, c)
		return
	}

	importRunId, err := m.AppDependencies.FileUploadService.UploadFile(
		file.Filename,
		fileBytes,
		int(fileSizeKb),
	)
	if err != nil {
		returnErrorJson(err, c)
		return
	}

	c.JSON(200, gin.H{
		"importRunId": importRunId.String(),
	})
}
